/*
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

package transport

import (
	"context"
	"time"
)

type Group interface {
	Set(context.Context, string, []byte, time.Time, bool) error
	Get(context.Context, string, Sink) error
	Remove(context.Context, string) error
	UsedBytes() (int64, int64)
	Name() string
	ExportCacheStats(which int) CacheStats
	ExportGroupStats() GroupStats
}

type CacheStats struct {
	Rejected  int64
	Bytes     int64
	Items     int64
	Gets      int64
	Hits      int64
	Evictions int64
}

// Definir GroupStats en el paquete transport
type GroupStats struct {
	Gets                     int64
	CacheHits                int64
	GetFromPeersLatencyLower int64
	PeerLoads                int64
	PeerErrors               int64
	Loads                    int64
	LoadsDeduped             int64
	LocalLoads               int64
	LocalLoadErrs            int64
}

// Constantes para el tipo de caché
const (
	MainCache = iota + 1
	HotCache
)
