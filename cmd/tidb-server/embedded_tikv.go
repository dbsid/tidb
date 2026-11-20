// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build tidbone

package main

import (
	"flag"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/store/embedded"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// Server labels that tell a proxy which TiKV store runs in this process.
const (
	storeAddrLabel = "tidb_one_store_addr"
	storeIDLabel   = "tidb_one_store_id"
)

// tidb-one: TiDB and TiKV in one process. The embedded TiKV starts when
// --tikv.addr is set; it joins the PD cluster given by --path.
var (
	embeddedTiKVConfig      = new(embedded.Config)
	embeddedTiKVReadyWait   *time.Duration
	embeddedTiKVStopTimeout *time.Duration
	embeddedTiKVLocalCall   *bool
	embeddedTiKVMaxInFlight *int64
	embeddedTiKVSyncWait    *time.Duration
	embeddedTiKVStore       *embedded.Store
)

func registerEmbeddedTiKVFlags(fset *flag.FlagSet) {
	c := embeddedTiKVConfig
	fset.StringVar(&c.ConfigFile, "tikv.config", "", "embedded TiKV: config file")
	fset.StringVar(&c.Addr, "tikv.addr", "", "embedded TiKV: gRPC listening address; setting it enables the embedded TiKV")
	fset.StringVar(&c.AdvertiseAddr, "tikv.advertise-addr", "", "embedded TiKV: advertised gRPC address")
	fset.StringVar(&c.StatusAddr, "tikv.status-addr", "", "embedded TiKV: status listening address")
	fset.StringVar(&c.AdvertiseStatusAddr, "tikv.advertise-status-addr", "", "embedded TiKV: advertised status address")
	fset.StringVar(&c.DataDir, "tikv.data-dir", "", "embedded TiKV: data directory")
	fset.StringVar(&c.LogFile, "tikv.log-file", "", "embedded TiKV: log file")
	fset.StringVar(&c.Labels, "tikv.labels", "", "embedded TiKV: store labels, k=v,k=v")
	fset.StringVar(&c.Capacity, "tikv.capacity", "", "embedded TiKV: store capacity")
	embeddedTiKVReadyWait = fset.Duration("tikv.ready-timeout", 10*time.Minute, "embedded TiKV: startup timeout")
	embeddedTiKVStopTimeout = fset.Duration("tikv.stop-timeout", time.Minute, "embedded TiKV: shutdown timeout")
	embeddedTiKVLocalCall = fset.Bool("tikv.local-call", true,
		"embedded TiKV: serve requests for the local store in process; false sends them over loopback gRPC (for comparison)")
	embeddedTiKVMaxInFlight = fset.Int64("tikv.local-max-inflight", embedded.DefaultMaxInFlight,
		"embedded TiKV: cap on in-flight local requests; requests above it use gRPC (0: no cap)")
	embeddedTiKVSyncWait = fset.Duration("tikv.local-sync-wait", embedded.DefaultSyncWait,
		"embedded TiKV: how long a local request holds its thread before it is answered through a callback")
}

// startEmbeddedTiKV starts the embedded TiKV and routes local requests to it.
// It must run before the TiKV storage is opened.
func startEmbeddedTiKV() {
	c := embeddedTiKVConfig
	if c.Addr == "" {
		return
	}
	if config.GetGlobalConfig().Store != config.StoreTypeTiKV {
		logutil.BgLogger().Fatal("embedded TiKV requires --store=tikv")
	}
	c.PDEndpoints = strings.Split(config.GetGlobalConfig().Path, ",")
	start := time.Now()
	logutil.BgLogger().Info("starting embedded TiKV", zap.Strings("args", c.Args()))
	s, err := embedded.Start(c, *embeddedTiKVReadyWait)
	if err != nil {
		logutil.BgLogger().Error("embedded TiKV failed to start", zap.Error(err))
		os.Exit(exitCodeErr)
	}
	embeddedTiKVStore = s
	s.SetMaxInFlight(*embeddedTiKVMaxInFlight)
	s.SetSyncWait(*embeddedTiKVSyncWait)
	if *embeddedTiKVLocalCall {
		tikv.SetDefaultLocalStore(s)
	} else {
		logutil.BgLogger().Warn("embedded TiKV local call is disabled; local requests use gRPC")
	}
	// Publish the co-located store in the server labels (visible in
	// /info/all and the etcd topology) so a proxy can send a transaction to
	// the TiDB that runs next to the Region leader it touches.
	config.UpdateGlobal(func(conf *config.Config) {
		labels := make(map[string]string, len(conf.Labels)+2)
		for k, v := range conf.Labels {
			labels[k] = v
		}
		labels[storeAddrLabel] = s.Addr()
		labels[storeIDLabel] = strconv.FormatUint(s.StoreID(), 10)
		conf.Labels = labels
	})
	logutil.BgLogger().Info("embedded TiKV is ready",
		zap.Uint64("store-id", s.StoreID()),
		zap.String("addr", s.Addr()),
		zap.Duration("took", time.Since(start)))
}

// stopEmbeddedTiKV stops the embedded TiKV after TiDB has closed its storage.
func stopEmbeddedTiKV() {
	if embeddedTiKVStore == nil {
		return
	}
	tikv.SetDefaultLocalStore(nil)
	if err := embeddedTiKVStore.Stop(true, *embeddedTiKVStopTimeout); err != nil {
		logutil.BgLogger().Warn("embedded TiKV did not stop cleanly", zap.Error(err))
	}
}
