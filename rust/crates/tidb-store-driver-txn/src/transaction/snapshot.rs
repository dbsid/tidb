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

//! Go `pkg/store/driver/txn.tikvSnapshot` for a statement's point read: the
//! store's snapshot at one version (`tikvStore.GetSnapshot`), with the
//! statement's options on it and its RPCs bound to the statement's call.

use std::sync::Arc;

use tidb_txnkv::driver::tikv_store::TikvStore;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::OptimisticCoordinatorError;
use tikv_client::{PdClient, Timestamp, TimestampExt};

use super::call_interceptor::CallSlot;
use super::client::{client_resource_group_tagger, SnapshotGetResult, TikvTxn};

/// One statement's read through the store's snapshot at `version`, with no
/// transaction around it, as Go's point read at `MaxUint64` reads.
pub(crate) struct TikvSnapshot<'a, S: PdClient> {
    store: &'a TikvStore<S>,
    snapshot: tikv_client::KvSnapshot<S>,
    version: u64,
    calls: CallSlot,
    /// The statement's runtime stats when it collects them, else the
    /// snapshot's own: the RPCs a read sends are counted here.
    stats: Arc<tikv_client::SnapshotRuntimeStats>,
    options: &'a tidb_txnkv::SnapshotReadOptions,
}

impl<'a, S: PdClient> TikvSnapshot<'a, S> {
    /// Go `tikvStore.GetSnapshot(version)` with the statement's `options`
    /// and the session's resource group set on it.
    pub(crate) fn open(
        store: &'a TikvStore<S>,
        version: u64,
        options: &'a tidb_txnkv::SnapshotReadOptions,
        resource_group_name: Option<&str>,
    ) -> Self {
        let calls = CallSlot::default();
        let mut snapshot = store.store().kv_snapshot(Timestamp::from_version(version));
        snapshot.add_rpc_interceptor(calls.interceptor());
        let stats = options
            .runtime_stats
            .as_ref()
            .map_or_else(Arc::default, |stats| Arc::clone(&stats.0));
        snapshot.set_snapshot_runtime_stats(Some(Arc::clone(&stats)));
        TikvTxn::<S>::apply_snapshot_read_options(&mut snapshot, options);
        if let Some(name) = resource_group_name {
            snapshot.set_resource_group_name(name);
        }
        Self {
            store,
            snapshot,
            version,
            calls,
            stats,
            options,
        }
    }

    /// Go `tikvSnapshot.Get` under the statement's `call`.
    pub(crate) fn get(
        mut self,
        key: &[u8],
        call: &UnaryCallContext,
    ) -> Result<SnapshotGetResult, OptimisticCoordinatorError> {
        self.calls.set(
            &call
                .clone()
                .with_rpc_interceptor(self.options.rpc_interceptor.clone()),
        );
        self.apply_statement_kv_context();
        let before = self.stats.rpc_count(tikv_client::SnapshotRpcCommand::Get);
        let value = self
            .store
            .block_on(self.snapshot.get(key.to_vec()))
            .map_err(TikvTxn::<S>::client_read_error)?;
        Ok(SnapshotGetResult {
            start_ts: self.version,
            value,
            rpc_count: self
                .stats
                .rpc_count(tikv_client::SnapshotRpcCommand::Get)
                .saturating_sub(before),
        })
    }

    /// The statement's KV context (Go's per-statement `SetOption` calls),
    /// as a transaction applies it before a read.
    fn apply_statement_kv_context(&mut self) {
        let Some(cell) = &self.options.kv_context else {
            return;
        };
        let context = cell.get();
        if let Some(ru_details) = &context.ru_details {
            self.snapshot.set_ru_details(Arc::clone(ru_details));
        }
        if let Some(exec_details) = &context.exec_details {
            self.snapshot.set_exec_details(Arc::clone(exec_details));
        }
        if let Some(tagger) = &context.resource_group_tagger {
            self.snapshot
                .set_resource_group_tagger(Some(client_resource_group_tagger(tagger)));
        }
        if let Some(source_type) = &context.request_source_type {
            self.snapshot.set_request_source_type(source_type.clone());
        }
        if let Some(name) = &context.resource_group_name {
            self.snapshot.set_resource_group_name(name.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    use tikv_client::mock::{MockKvClient, MockPdClient};
    use tikv_client::proto::kvrpcpb;

    use super::*;

    /// Go's point read at `MaxUint64` reads through the store's snapshot with
    /// the statement's options: the request carries the statement KV
    /// context's resource group and request source, the statement's
    /// interceptor (TopSQL's KV execution counter) sees the call even when
    /// the session also names a resource group, and the read counts its one
    /// RPC.
    #[test]
    fn a_max_ts_point_read_carries_the_statement_options() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request.downcast_ref::<kvrpcpb::GetRequest>().unwrap();
                captured.lock().unwrap().push(request.clone());
                Ok(Box::new(kvrpcpb::GetResponse {
                    value: b"value".to_vec(),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let runtime = tidb_txnkv::driver::tikv_store::store_runtime().unwrap();
        let inner = runtime
            .block_on(tikv_client::tikv::KvStore::new_with_pd_client(
                pd_client,
                Arc::new(tikv_client::tikv::MockSafePointKv::new()),
                tikv_client::Config::default(),
            ))
            .unwrap();
        let store = TikvStore::new(Arc::clone(&runtime), inner);

        let intercepted = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&intercepted);
        let interceptor = tikv_client::new_rpc_interceptor("statement", move |_, _, next| {
            counter.fetch_add(1, Ordering::SeqCst);
            next()
        });
        let kv_context = tidb_txnkv::StatementKvContextCell::default();
        kv_context.publish(tidb_txnkv::StatementKvContext {
            resource_group_name: Some("rg".to_owned()),
            request_source_type: Some("lightning".to_owned()),
            ..Default::default()
        });
        let options = tidb_txnkv::SnapshotReadOptions {
            rpc_interceptor: tidb_txnkv::RpcInterceptorOption::new(interceptor),
            kv_context: Some(kv_context),
            ..Default::default()
        };
        let call = UnaryCallContext::new(
            Duration::from_secs(10),
            tidb_txnkv::UnaryCancellation::new(),
        );

        let result = TikvSnapshot::open(&store, u64::MAX, &options, Some("session_rg"))
            .get(b"key", &call)
            .unwrap();

        assert_eq!(result.value, Some(b"value".to_vec()));
        assert_eq!(result.rpc_count, 1);
        assert_eq!(intercepted.load(Ordering::SeqCst), 1);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].version, u64::MAX);
        let context = requests[0].context.as_ref().unwrap();
        assert_eq!(
            context
                .resource_control_context
                .as_ref()
                .unwrap()
                .resource_group_name,
            "rg"
        );
        assert!(
            context.request_source.ends_with("lightning"),
            "request source {:?}",
            context.request_source
        );
    }
}
