/*
Copyright 2012 Google Inc.
Copyright Derrick J Wippler

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package groupcache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/groupcache/groupcache-go/v3/internal/singleflight"
	"github.com/groupcache/groupcache-go/v3/transport"
	"github.com/groupcache/groupcache-go/v3/transport/pb"
	"github.com/groupcache/groupcache-go/v3/transport/peer"
)

// Group is the user facing interface for a group
type Group interface {
	// TODO: deprecate the hotCache boolean in Set(). It is not needed

	Set(context.Context, string, []byte, time.Time, bool) error
	Get(context.Context, string, transport.Sink) error
	Remove(context.Context, string) error
	UsedBytes() (int64, int64)
	Name() string
	ExportCacheStats(which int) transport.CacheStats
	ExportGroupStats() transport.GroupStats
}

// A Getter loads data for a key.
type Getter interface {
	// Get returns the value identified by key, populating dest.
	//
	// The returned data must be unversioned. That is, key must
	// uniquely describe the loaded data, without an implicit
	// current time, and without relying on cache expiration
	// mechanisms.
	Get(ctx context.Context, key string, dest transport.Sink) error
}

// A GetterFunc implements Getter with a function.
type GetterFunc func(ctx context.Context, key string, dest transport.Sink) error

func (f GetterFunc) Get(ctx context.Context, key string, dest transport.Sink) error {
	return f(ctx, key, dest)
}

// A Group is a cache namespace and associated data loaded spread over
// a group of 1 or more machines.
type group struct {
	name          string
	getter        Getter
	instance      *Instance
	maxCacheBytes int64 // max size of both mainCache and hotCache

	// mainCache is a cache of the keys for which this process
	// (amongst its peers) is authoritative. That is, this cache
	// contains keys which consistent hash on to this process's
	// peer number.
	mainCache Cache

	// hotCache contains keys/values for which this peer is not
	// authoritative (otherwise they would be in mainCache), but
	// are popular enough to warrant mirroring in this process to
	// avoid going over the network to fetch from a peer.  Having
	// a hotCache avoids network hot spotting, where a peer's
	// network card could become the bottleneck on a popular key.
	// This cache is used sparingly to maximize the total number
	// of key/value pairs that can be stored globally.
	hotCache Cache

	// loadGroup ensures that each key is only fetched once
	// (either locally or remotely), regardless of the number of
	// concurrent callers.
	loadGroup *singleflight.Group

	// setGroup ensures that each added key is only added
	// remotely once regardless of the number of concurrent callers.
	setGroup *singleflight.Group

	// removeGroup ensures that each removed key is only removed
	// remotely once regardless of the number of concurrent callers.
	removeGroup *singleflight.Group

	// Stats are statistics on the group.
	Stats GroupStats
}

// Name returns the name of the group.
func (g *group) Name() string {
	return g.name
}

// UsedBytes returns the total number of bytes used by the main and hot caches
func (g *group) UsedBytes() (mainCache int64, hotCache int64) {
	return g.mainCache.Bytes(), g.hotCache.Bytes()
}

func (g *group) ExportCacheStats(which int) transport.CacheStats {
	stats := g.CacheStats(CacheType(which))
	return transport.CacheStats{
		Rejected:  stats.Rejected,
		Bytes:     stats.Bytes,
		Items:     stats.Items,
		Gets:      stats.Gets,
		Hits:      stats.Hits,
		Evictions: stats.Evictions,
	}
}

func (g *group) ExportGroupStats() transport.GroupStats {
	return transport.GroupStats{
		Gets:                     g.Stats.Gets.Get(),
		CacheHits:                g.Stats.CacheHits.Get(),
		GetFromPeersLatencyLower: g.Stats.GetFromPeersLatencyLower.Get(),
		PeerLoads:                g.Stats.PeerLoads.Get(),
		PeerErrors:               g.Stats.PeerErrors.Get(),
		Loads:                    g.Stats.Loads.Get(),
		LoadsDeduped:             g.Stats.LoadsDeduped.Get(),
		LocalLoads:               g.Stats.LocalLoads.Get(),
		LocalLoadErrs:            g.Stats.LocalLoadErrs.Get(),
	}
}

func (g *group) Get(ctx context.Context, key string, dest transport.Sink) error {
	g.Stats.Gets.Add(1)
	if dest == nil {
		return errors.New("groupcache: nil dest Sink")
	}
	value, cacheHit := g.lookupCache(key)

	if cacheHit {
		g.Stats.CacheHits.Add(1)
		return transport.SetSinkView(dest, value)
	}

	// Optimization to avoid double unmarshalling or copying: keep
	// track of whether the dest was already populated. One caller
	// (if local) will set this; the losers will not. The common
	// case will likely be one caller.
	var destPopulated bool
	value, destPopulated, err := g.load(ctx, key, dest)
	if err != nil {
		return err
	}
	if destPopulated {
		return nil
	}
	return transport.SetSinkView(dest, value)
}

func (g *group) Set(ctx context.Context, key string, value []byte, expire time.Time, _ bool) error {
	if key == "" {
		return errors.New("empty Set() key not allowed")
	}

	// Si no hay tamaño de caché, no hacemos nada
	if g.maxCacheBytes <= 0 {
		return nil
	}

	// Usamos singleflight para asegurar que solo una operación Set para la misma clave
	// esté activa a la vez en este nodo (para la lógica de coordinación y actualización local/remota).
	_, err := g.setGroup.Do(key, func() (interface{}, error) {

		// *** INICIO: Crear una copia del valor ***
		// Se crea una copia explícita del slice de bytes.
		// Esto es crucial para evitar que las goroutines que actualizan los peers
		// retengan referencias al slice 'value' original o a versiones antiguas,
		// permitiendo que el GC libere la memoria de ciclos anteriores.
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)
		// *** FIN: Crear una copia del valor ***

		// Determinar el nodo "dueño" de la clave según el hash consistente.
		owner, isRemote := g.instance.PickPeer(key)

		// Si el dueño es un nodo remoto...
		if isRemote {
			// ...enviar la operación Set al dueño (usando la copia).
			// Esta llamada es síncrona dentro de la función de singleflight.Do.
			if err := g.setPeer(ctx, owner, key, valueCopy, expire); err != nil { // <-- Usa valueCopy
				// Si falla la comunicación con el dueño, retornamos error.
				// La política aquí podría variar (¿continuar actualizando otros peers?).
				// Actualmente, si falla con el dueño, la operación Set completa falla.
				return nil, err
			}
		}

		// Actualizar las cachés locales (mainCache y hotCache) de este nodo.
		// Se usa la copia del valor para crear la ByteView.
		bv := transport.ByteViewWithExpire(valueCopy, expire) // <-- Usa valueCopy

		g.loadGroup.Lock(func() {
			if g.isOwner(key) {
				g.mainCache.Add(key, bv)
				g.hotCache.Remove(key)
			} else {
				g.hotCache.Add(key, bv)
			}
		})

		// Actualizar todos los demás peers en el clúster (excepto este nodo y el dueño).
		var wg sync.WaitGroup
		for _, p := range g.instance.getAllPeers() {
			// Saltar la actualización a sí mismo.
			if p.PeerInfo().IsSelf {
				continue
			}

			// Saltar la actualización al dueño (ya se hizo si era remoto,
			// y si era local, la actualización local ya ocurrió).
			if p.HashKey() == owner.HashKey() {
				continue
			}

			// Incrementar el contador del WaitGroup para esta goroutine.
			wg.Add(1)
			// Lanzar una goroutine para actualizar a este peer de forma asíncrona.
			go func(p peer.Client) {
				// *** INICIO: Asegurar wg.Done() y Recuperar Pánico ***
				// defer wg.Done() es crucial para asegurar que Wait() no se bloquee
				// indefinidamente si g.setPeer panica.
				defer wg.Done()

				// Opcional pero recomendado: Recuperar pánicos dentro de la goroutine
				// para loguearlos y evitar que el programa entero caiga.
				defer func() {
					if r := recover(); r != nil {
						g.instance.opts.Logger.Error("PANIC during setPeer",
							"peer", p.PeerInfo().Address,
							"key", key,
							"panic", fmt.Sprintf("%v", r),
							// Considerar loguear stack trace: "stack", string(debug.Stack()),
						)
					}
				}()
				// *** FIN: Asegurar wg.Done() y Recuperar Pánico ***

				// Llamar a setPeer para enviar la operación Set al peer (usando la copia).
				// La goroutine captura 'valueCopy', que es la copia específica de esta ejecución de Set.
				if err := g.setPeer(ctx, p, key, valueCopy, expire); err != nil { // <-- Usa valueCopy
					// Loguear errores de comunicación con el peer, pero no hacer fallar
					// la operación Set principal (es un esfuerzo "best-effort").
					g.instance.opts.Logger.Error("while calling Set on peer",
						"peer", p.PeerInfo().Address,
						"key", key,
						"err", err)
				}
			}(p) // Pasar el peer 'p' como argumento a la goroutine
		}
		// Esperar a que todas las goroutines de actualización de peers terminen.
		// Esto asegura que la llamada a group.Set no retorne hasta que se haya
		// intentado actualizar a todos los peers.
		wg.Wait()

		// La función de singleflight.Do retorna nil en caso de éxito.
		return nil, nil
	}) // Fin de singleflight.Do

	// Retornar el error de singleflight.Do (si lo hubo).
	return err
}

// Remove clears the key from our cache then forwards the remove
// request to all peers.
//
// ### Consistency Warning
// This method implements a best case design since it is possible a temporary network disruption could
// occur resulting in remove requests never making it their peers. In practice this scenario is rare
// and the system typically remains consistent. However, in case of an inconsistency we recommend placing
// an expiration time on your values to ensure the cluster eventually becomes consistent again.
func (g *group) Remove(ctx context.Context, key string) error {
	_, err := g.removeGroup.Do(key, func() (interface{}, error) {

		// Remove from key owner first
		owner, isRemote := g.instance.PickPeer(key)
		if isRemote {
			if err := g.removeFromPeer(ctx, owner, key); err != nil {
				// Si falla la eliminación en el dueño, retornamos error.
				// Podría considerarse continuar con los otros peers, pero
				// actualmente la operación Remove completa falla.
				return nil, err
			}
		}
		// Remove from our cache next
		g.LocalRemove(key) // Elimina de mainCache y hotCache locales

		// --- INICIO MODIFICACIÓN ---
		wg := sync.WaitGroup{}
		// Usamos un buffered channel para evitar bloqueos si hay muchos errores rápidos
		// y el lector (más abajo) no es lo suficientemente rápido. El tamaño puede ajustarse.
		numPeersToNotify := 0
		for _, p := range g.instance.getAllPeers() {
			if p != owner { // Contar cuántos peers necesitan notificación
				numPeersToNotify++
			}
		}
		// Crear canal con buffer suficiente para todos los posibles errores + 1 (por si acaso)
		errCh := make(chan error, numPeersToNotify+1)

		// Asynchronously clear the key from all hot and main caches of peers
		for _, p := range g.instance.getAllPeers() {
			// avoid deleting from owner a second time
			if p == owner {
				continue
			}
			// Saltar a sí mismo (LocalRemove ya lo hizo)
			if p.PeerInfo().IsSelf {
				continue
			}

			wg.Add(1)
			go func(p peer.Client) {
				// *** AÑADIR defer wg.Done() ***
				// Asegura que wg.Done() se llame incluso si removeFromPeer panica.
				defer wg.Done()

				// *** Opcional pero recomendado: Añadir recover() ***
				defer func() {
					if r := recover(); r != nil {
						// Loguear el pánico
						g.instance.opts.Logger.Error("PANIC during removeFromPeer",
							"peer", p.PeerInfo().Address,
							"key", key,
							"panic", fmt.Sprintf("%v", r),
							// Considerar loguear stack trace: "stack", string(debug.Stack()),
						)
						// Opcionalmente, enviar un error específico al canal
						// errCh <- fmt.Errorf("panic during removeFromPeer for peer %s: %v", p.PeerInfo().Address, r)
					}
				}()

				// Llamar a removeFromPeer y enviar el resultado (error o nil) al canal
				err := g.removeFromPeer(ctx, p, key)
				if err != nil {
					// Loguear el error específico de este peer
					g.instance.opts.Logger.Error("while calling Remove on peer",
						"peer", p.PeerInfo().Address,
						"key", key,
						"err", err)
				}
				// Enviar el error (puede ser nil) al canal para agregación
				errCh <- err

				// wg.Done() // Ya no es necesario aquí explícitamente
			}(p) // Pasar el peer 'p' como argumento
		}

		// Goroutine para esperar a que todas las llamadas a removeFromPeer terminen y luego cerrar el canal de errores
		go func() {
			wg.Wait()
			close(errCh)
		}()
		// --- FIN MODIFICACIÓN ---

		// Recolectar todos los errores del canal
		m := &MultiError{} // Asumiendo que tienes una estructura MultiError o similar
		for err := range errCh {
			if err != nil { // Solo añadir errores reales
				m.Add(err)
			}
		}

		// Retornar nil si no hubo errores, o el MultiError si los hubo
		return nil, m.NilOrError() // Asumiendo que NilOrError() devuelve nil si no hay errores
	})
	return err
}

// load loads key either by invoking the getter locally or by sending it to another machine.
func (g *group) load(ctx context.Context, key string, dest transport.Sink) (value transport.ByteView, destPopulated bool, err error) {
	g.Stats.Loads.Add(1)
	viewi, err := g.loadGroup.Do(key, func() (interface{}, error) {
		// Check the cache again because singleflight can only dedup calls
		// that overlap concurrently.  It's possible for 2 concurrent
		// requests to miss the cache, resulting in 2 load() calls.  An
		// unfortunate goroutine scheduling would result in this callback
		// being run twice, serially.  If we don't check the cache again,
		// cache.nbytes would be incremented below even though there will
		// be only one entry for this key.
		//
		// Consider the following serialized event ordering for two
		// goroutines in which this callback gets called twice for hte
		// same key:
		// 1: Get("key")
		// 2: Get("key")
		// 1: lookupCache("key")
		// 2: lookupCache("key")
		// 1: load("key")
		// 2: load("key")
		// 1: loadGroup.Do("key", fn)
		// 1: fn()
		// 2: loadGroup.Do("key", fn)
		// 2: fn()
		if value, cacheHit := g.lookupCache(key); cacheHit {
			g.Stats.CacheHits.Add(1)
			return value, nil
		}
		g.Stats.LoadsDeduped.Add(1)
		var value transport.ByteView
		var err error
		if peer, ok := g.instance.PickPeer(key); ok {

			// metrics duration start
			start := time.Now()

			// get value from peers
			value, err = g.getFromPeer(ctx, peer, key)

			// metrics duration compute
			duration := int64(time.Since(start)) / int64(time.Millisecond)

			// metrics only store the slowest duration
			if g.Stats.GetFromPeersLatencyLower.Get() < duration {
				g.Stats.GetFromPeersLatencyLower.Store(duration)
			}

			if err == nil {
				g.Stats.PeerLoads.Add(1)
				return value, nil
			}

			if errors.Is(err, context.Canceled) {
				return nil, err
			}

			if errors.Is(err, &transport.ErrNotFound{}) {
				return nil, err
			}

			if errors.Is(err, &transport.ErrRemoteCall{}) {
				return nil, err
			}

			if g.instance.opts.Logger != nil {
				g.instance.opts.Logger.Error(
					"while retrieving key from peer",
					"peer", peer.PeerInfo().Address,
					"category", "groupcache",
					"err", err,
					"key", key)
			}

			g.Stats.PeerErrors.Add(1)
			if ctx != nil && ctx.Err() != nil {
				// Return here without attempting to get locally
				// since the context is no longer valid
				return nil, err
			}
		}

		value, err = g.getLocally(ctx, key, dest)
		if err != nil {
			g.Stats.LocalLoadErrs.Add(1)
			return nil, err
		}
		g.Stats.LocalLoads.Add(1)
		destPopulated = true // only one caller of load gets this return value
		g.populateCache(key, value, g.mainCache)
		return value, nil
	})
	if err == nil {
		value = viewi.(transport.ByteView)
	}
	return
}

func (g *group) getLocally(ctx context.Context, key string, dest transport.Sink) (transport.ByteView, error) {
	err := g.getter.Get(ctx, key, dest)
	if err != nil {
		return transport.ByteView{}, err
	}
	return dest.View()
}

func (g *group) getFromPeer(ctx context.Context, peer peer.Client, key string) (transport.ByteView, error) {
	req := &pb.GetRequest{
		Group: &g.name,
		Key:   &key,
	}
	res := &pb.GetResponse{}
	err := peer.Get(ctx, req, res)
	if err != nil {
		return transport.ByteView{}, err
	}

	var expire time.Time
	if res.Expire != nil && *res.Expire != 0 {
		expire = time.Unix(*res.Expire/int64(time.Second), *res.Expire%int64(time.Second))
	}

	value := transport.ByteViewWithExpire(res.Value, expire)

	// Always populate the hot cache
	g.populateCache(key, value, g.hotCache)
	return value, nil
}

func (g *group) setPeer(ctx context.Context, peer peer.Client, k string, v []byte, e time.Time) error {
	var expire int64
	if !e.IsZero() {
		expire = e.UnixNano()
	}
	req := &pb.SetRequest{
		Expire: &expire,
		Group:  &g.name,
		Key:    &k,
		Value:  v,
	}
	return peer.Set(ctx, req)
}

func (g *group) removeFromPeer(ctx context.Context, peer peer.Client, key string) error {
	req := &pb.GetRequest{
		Group: &g.name,
		Key:   &key,
	}
	return peer.Remove(ctx, req)
}

func (g *group) lookupCache(key string) (value transport.ByteView, ok bool) {
	if g.maxCacheBytes <= 0 {
		return
	}
	value, ok = g.mainCache.Get(key)
	if ok {
		return
	}
	value, ok = g.hotCache.Get(key)
	return
}

func (g *group) isOwner(key string) bool {
	// PickPeer devuelve (cliente, isRemote)
	// Si isRemote == false, este nodo es el dueño.
	_, remote := g.instance.PickPeer(key)
	return !remote
}

// RemoteSet is called by the transport to set values in the local and hot caches when
// a remote peer sends us a pb.SetRequest
/*func (g *group) RemoteSet(key string, value []byte, expire time.Time) {
	if g.maxCacheBytes <= 0 {
		return
	}

	// Lock all load operations until this function returns
	g.loadGroup.Lock(func() {
		// This instance could take over ownership of this key at any moment after
		// the set is made. In order to avoid accidental propagation of the previous
		// value should this instance become owner of the key, we always set key in
		// the main cache.
		bv := transport.ByteViewWithExpire(value, expire)
		g.mainCache.Add(key, bv)

		// It's possible the value could be in the hot cache.
		g.hotCache.Remove(key)
	})
}*/

func (g *group) RemoteSet(key string, value []byte, expire time.Time) {
	if g.maxCacheBytes <= 0 {
		return
	}

	// Construimos el ByteView una sola vez
	bv := transport.ByteViewWithExpire(value, expire)

	// Bloqueamos los Gets mientras actualizamos las cachés locales
	g.loadGroup.Lock(func() {
		if g.isOwner(key) {
			// Este nodo es el propietario ► almacena en mainCache
			g.mainCache.Add(key, bv)
			// Por si la clave ya estaba de paso en hotCache
			g.hotCache.Remove(key)
		} else {
			// Nodo no-dueño ► replica en hotCache (límite 1/8)
			g.hotCache.Add(key, bv)
			// Nunca la guardamos en mainCache aquí
		}
	})
}

func (g *group) LocalRemove(key string) {
	// Clear key from our local cache
	if g.maxCacheBytes <= 0 {
		return
	}

	// Ensure no requests are in flight
	g.loadGroup.Lock(func() {
		g.hotCache.Remove(key)
		g.mainCache.Remove(key)
	})
}

func (g *group) populateCache(key string, value transport.ByteView, cache Cache) {
	if g.maxCacheBytes <= 0 {
		return
	}
	cache.Add(key, value)
}

// CacheType represents a type of cache.
type CacheType int

const (
	// The MainCache is the cache for items that this peer is the
	// owner for.
	MainCache CacheType = iota + 1

	// The HotCache is the cache for items that seem popular
	// enough to replicate to this node, even though it's not the
	// owner.
	HotCache
)

// CacheStats returns stats about the provided cache within the group.
func (g *group) CacheStats(which CacheType) CacheStats {
	switch which {
	case MainCache:
		return g.mainCache.Stats()
	case HotCache:
		return g.hotCache.Stats()
	default:
		return CacheStats{}
	}
}

// ResetCacheSize changes the maxBytes allowed and resets both the main and hot caches.
// It is mostly intended for testing and is not thread safe.
func (g *group) ResetCacheSize(maxBytes int64) error {
	g.maxCacheBytes = maxBytes
	var (
		hotCache  int64
		mainCache int64
	)

	// Avoid divide by zero
	if maxBytes >= 0 {
		// Hot cache is 1/8th the size of the main cache
		hotCache = maxBytes / 8
		mainCache = hotCache * 7
	}

	var err error
	g.mainCache, err = g.instance.opts.CacheFactory(mainCache)
	if err != nil {
		return fmt.Errorf("Options.CacheFactory(): %w", err)
	}
	g.hotCache, err = g.instance.opts.CacheFactory(hotCache)
	if err != nil {
		return fmt.Errorf("Options.CacheFactory(): %w", err)
	}
	return nil
}
