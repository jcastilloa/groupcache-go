/*
Copyright 2012 Google Inc.
Copyright 2024 Derrick J Wippler

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

package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/groupcache/groupcache-go/v3/transport/pb"
	"github.com/groupcache/groupcache-go/v3/transport/peer"
	"golang.org/x/net/proxy"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultBasePath = "/_groupcache/"
	defaultScheme   = "http"
)

var bufferPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

const maxBufferSize = 1024 * 1024 // 1MB

type GroupCacheInstance interface {
	GetGroup(string) Group
}

type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

type Transport interface {
	// New returns a clone of this instance suitable for passing to groupcache.New()
	// Example usage:
	//
	// transport := groupcache.NewHttpTransport(groupcache.HttpTransportOptions{})
	// groupcache.New(groupcache.Config{Transport: transport.New()})
	New() Transport

	// Register registers the provided *Instance with the HttpTransport.
	//
	// This method sets the instance field of the HttpTransport to the provided instance.
	// The instance is used by the ServeHTTP method to serve groupcache requests.
	Register(instance GroupCacheInstance)

	// NewClient returns a new Client suitable for the transport implementation. The client returned is used to communicate
	// with a specific peer. This method will be called for each peer in the peer list when groupcache.Instance.SetPeers() is
	// called.
	NewClient(ctx context.Context, peer peer.Info) (peer.Client, error)

	// ListenAndServe spawns a server that will handle incoming requests for this transport
	// This is used by daemon and cluster packages to create a cluster of instances using
	// this specific transport.
	ListenAndServe(ctx context.Context, address string) error

	// Shutdown shuts down the server started when calling ListenAndServe()
	Shutdown(ctx context.Context) error

	// ListenAddress returns the address the server is listening on after calling ListenAndServe().
	ListenAddress() string
}

// HttpTransportOptions options for creating a new HttpTransport
type HttpTransportOptions struct {
	// Context (Optional) specifies a context for the server to use when it
	// receives a request.
	// defaults to http.Request.Context()
	Context func(*http.Request) context.Context

	// Client (Optional) provide a custom http client with TLS config.
	// defaults to http.DefaultClient
	Client *http.Client

	// Scheme (Optional) is either `http` or `https`. `Scheme` is reserved here for future use.
	// defaults to `http` when TLSConfig is not set.
	Scheme string

	// BasePath (Optional) specifies the HTTP path that will serve groupcache requests.
	// defaults to "/_groupcache/".
	BasePath string

	// Logger
	Logger Logger

	// TLS support.
	TLSConfig *tls.Config
}

// HttpTransport defines the HTTP transport
type HttpTransport struct {
	opts     HttpTransportOptions
	instance GroupCacheInstance
	wg       sync.WaitGroup
	listener net.Listener
	server   *http.Server
}

// NewHttpTransport returns a new HttpTransport instance based on the provided HttpTransportOptions.
// Example usage:
//
//		transport := groupcache.NewHttpTransport(groupcache.HttpTransportOptions{
//		   BasePath: "/_groupcache/",
//		   Scheme:   "http",
//		   Client:   nil,
//		})
//
//	 instance := groupcache.New(.....)
//
//	 // Must register the groupcache instance before using transport
//	 transport.Register(instance)
func NewHttpTransport(opts HttpTransportOptions) *HttpTransport {
	if opts.BasePath == "" {
		opts.BasePath = DefaultBasePath
	}

	if opts.Scheme == "" {
		opts.Scheme = defaultScheme
	}

	if opts.Client == nil {
		opts.Client = http.DefaultClient
	}

	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	// override the Scheme that is set to ensure it is https
	if opts.TLSConfig != nil {
		opts.Scheme = "https"
	}

	return &HttpTransport{
		opts: opts,
	}
}

func (t *HttpTransport) tls() bool {
	return t.opts.TLSConfig != nil
}

// Register registers the provided instance with this transport.
func (t *HttpTransport) Register(instance GroupCacheInstance) {
	t.instance = instance
}

// New creates a new unregistered HttpTransport, using the same options as its parent.
func (t *HttpTransport) New() Transport {
	return NewHttpTransport(t.opts)
}

// ListenAndServe starts a new http server listening on the provided address:port
func (t *HttpTransport) ListenAndServe(ctx context.Context, address string) error {
	mux := http.NewServeMux()
	mux.Handle(t.opts.BasePath, t)

	var err error

	t.listener, err = net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("while starting HTTP listener: %w", err)
	}

	if t.tls() {
		t.listener = tls.NewListener(t.listener, t.opts.TLSConfig)
	}

	t.server = &http.Server{
		Handler: mux,
	}

	t.wg.Add(1)
	go func() {
		t.opts.Logger.Info(fmt.Sprintf("Listening on %s ....", address))

		if err := t.server.Serve(t.listener); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				var proto string
				if t.tls() {
					proto = "HTTPS"
				} else {
					proto = "HTTP"
				}
				t.opts.Logger.Error(fmt.Sprintf("while starting %s server", proto), "err", err)
			}
		}

		t.wg.Done()
	}()

	// Ensure server is accepting connections before returning
	return waitForConnect(ctx, t.listener.Addr().String(), t.opts.TLSConfig)
}

// Shutdown shuts down the server started when calling ListenAndServe()
func (t *HttpTransport) Shutdown(ctx context.Context) error {
	if err := t.server.Shutdown(ctx); err != nil {
		return err
	}
	t.wg.Wait()
	return nil
}

// ListenAddress returns the address the server is listening on after calling ListenAndServe().
func (t *HttpTransport) ListenAddress() string {
	return t.listener.Addr().String()
}

// ServeHTTP handles all incoming HTTP requests received by the server spawned by ListenAndServe()
func (t *HttpTransport) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if t.instance == nil {
		panic("groupcache instance is nil; you must register an instance by calling HttpTransport.Register()")
	}

	if !strings.HasPrefix(r.URL.Path, t.opts.BasePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}

	parts := strings.SplitN(r.URL.Path[len(t.opts.BasePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	groupName := parts[0]
	key := parts[1]

	type transportMethods interface {
		Get(ctx context.Context, key string, dest Sink) error
		RemoteSet(string, []byte, time.Time)
		LocalRemove(string)
	}

	// Fetch the value for this group/key.
	group := t.instance.GetGroup(groupName).(transportMethods)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}
	var ctx context.Context
	if t.opts.Context != nil {
		ctx = t.opts.Context(r)
	} else {
		ctx = r.Context()
	}

	// Delete the key and return 200
	if r.Method == http.MethodDelete {
		group.LocalRemove(key)
		return
	}

	// The read the body and set the key value
	if r.Method == http.MethodPut {
		defer r.Body.Close()
		b := bufferPool.Get().(*bytes.Buffer)
		b.Reset()
		defer func() {
			if b.Cap() <= maxBufferSize {
				bufferPool.Put(b)
			}
		}()
		_, err := io.Copy(b, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var out pb.SetRequest
		err = proto.Unmarshal(b.Bytes(), &out)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var expire time.Time
		if out.Expire != nil && *out.Expire != 0 {
			expire = time.Unix(*out.Expire/int64(time.Second), *out.Expire%int64(time.Second))
		}
		val := make([]byte, len(out.Value))
		copy(val, out.Value)
		group.RemoteSet(*out.Key, val, expire)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "Only GET, DELETE, PUT are supported", http.StatusMethodNotAllowed)
		return
	}

	var b []byte

	value := AllocatingByteSliceSink(&b)
	err := group.Get(ctx, key, value)
	if err != nil {
		if errors.Is(err, &ErrNotFound{}) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	view, err := value.View()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var expireNano int64
	if !view.Expire().IsZero() {
		expireNano = view.Expire().UnixNano()
	}

	// Write the value to the response body as a proto message.
	body, err := proto.Marshal(&pb.GetResponse{Value: b, Expire: &expireNano})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(body)
}

// NewClient creates a new http client for the provided peer
func (t *HttpTransport) NewClient(_ context.Context, p peer.Info) (peer.Client, error) {
	return &HttpClient{
		endpoint: fmt.Sprintf("%s://%s%s", t.opts.Scheme, p.Address, t.opts.BasePath),
		client:   t.opts.Client,
		info:     p,
	}, nil
}

// HttpClient represents an HTTP client used to make requests to a specific peer.
type HttpClient struct {
	// Peer information for this client
	info peer.Info
	// The address of endpoint in the format `<scheme>://<host>:<port>`
	endpoint string
	// The http client used to make requests
	client *http.Client
}

// Get recupera la clave <in.Group>/<in.Key> desde el peer remoto y
// coloca el resultado en 'out'.  Se asegura de que el buffer que
// viene del sync.Pool NO quede referenciado por la caché.
func (h *HttpClient) Get(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	// ---------- 1. lanzar la petición --------------------------
	var res http.Response
	if err := h.makeRequest(ctx, http.MethodGet, in, nil, &res); err != nil {
		return err
	}
	defer res.Body.Close()

	// ---------- 2. gestionar códigos de error ------------------
	if res.StatusCode != http.StatusOK {
		// leemos (máx. 1 MiB) el cuerpo de error para incluirlo en el msg
		msg, _ := io.ReadAll(io.LimitReader(res.Body, 1024*1024))

		switch res.StatusCode {
		case http.StatusNotFound:
			return &ErrNotFound{Msg: strings.Trim(string(msg), "\n")}
		case http.StatusServiceUnavailable:
			return &ErrRemoteCall{Msg: strings.Trim(string(msg), "\n")}
		default:
			return fmt.Errorf("server returned: %v, %v", res.Status, string(msg))
		}
	}

	// ---------- 3. leer la respuesta en un *bytes.Buffer -------
	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()
	defer func() {
		// devolvemos el buffer al pool SOLO si no es gigante
		if b.Cap() <= maxBufferSize {
			bufferPool.Put(b)
		}
	}()

	if _, err := io.Copy(b, res.Body); err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}

	// ---------- 4. des-serializar el proto ---------------------
	if err := proto.Unmarshal(b.Bytes(), out); err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}

	// ---------- 5. copia defensiva para romper la referencia ---
	// out.Value apunta al array interno de 'b'.  Copiamos para
	// que la caché no retenga ese array y el buffer pueda
	// reutilizarse / ser liberado por el GC.
	if out.Value != nil {
		val := make([]byte, len(out.Value))
		copy(val, out.Value)
		out.Value = val
	}

	return nil
}

func (h *HttpClient) Set(ctx context.Context, in *pb.SetRequest) error {
	body, err := proto.Marshal(in)
	if err != nil {
		return fmt.Errorf("while marshaling SetRequest body: %w", err)
	}
	var res http.Response
	if err := h.makeRequest(ctx, http.MethodPut, in, bytes.NewReader(body), &res); err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("while reading body response: %v", res.Status)
		}
		return fmt.Errorf("server returned status %d: %s", res.StatusCode, body)
	}
	return nil
}

func (h *HttpClient) Remove(ctx context.Context, in *pb.GetRequest) error {
	var res http.Response
	if err := h.makeRequest(ctx, http.MethodDelete, in, nil, &res); err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("while reading body response: %v", res.Status)
		}
		return fmt.Errorf("server returned status %d: %s", res.StatusCode, body)
	}
	return nil
}

func (h *HttpClient) PeerInfo() peer.Info {
	return h.info
}

func (h *HttpClient) HashKey() string {
	return h.info.Address
}

type request interface {
	GetGroup() string
	GetKey() string
}

func (h *HttpClient) makeRequest(ctx context.Context, m string, in request, b io.Reader, out *http.Response) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.endpoint,
		url.PathEscape(in.GetGroup()),
		url.PathEscape(in.GetKey()),
	)

	req, err := http.NewRequestWithContext(ctx, m, u, b)
	if err != nil {
		return err
	}

	res, err := h.client.Do(req)
	if err != nil {
		return err
	}
	*out = *res
	return nil
}

// waitForConnect waits until the passed address is accepting connections.
// It will continue to attempt a connection until context is canceled.
func waitForConnect(ctx context.Context, address string, cfg *tls.Config) error {
	if address == "" {
		return fmt.Errorf("waitForConnect() requires a valid address")
	}

	var errs []string
	for {
		var d proxy.ContextDialer
		if cfg != nil {
			d = &tls.Dialer{Config: cfg}
		} else {
			d = &net.Dialer{}
		}
		conn, err := d.DialContext(ctx, "tcp", address)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		errs = append(errs, err.Error())
		if ctx.Err() != nil {
			errs = append(errs, ctx.Err().Error())
			return errors.New(strings.Join(errs, "\n"))
		}
		time.Sleep(time.Millisecond * 100)
		continue
	}
}
