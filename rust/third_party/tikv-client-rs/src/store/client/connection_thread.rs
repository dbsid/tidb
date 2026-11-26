// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Experimental read transport owned by a synchronous SQL connection.

use std::any::Any;
use std::cell::{OnceCell, RefCell};
use std::future::{poll_fn, Future};
use std::rc::Rc;
use std::sync::{atomic::AtomicBool, Arc};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use tokio::runtime::{Builder, Handle, Runtime};

use super::{ConnectionInfo, KvClient, KvRpcClient, TikvConnect};
use crate::proto::kvrpcpb;
use crate::store::Request;
use crate::{Error, Result};

#[derive(Default)]
struct Connection {
    // Drop the last private pool before shutting down its reactor.
    pool: RefCell<Option<(Arc<ConnectionInfo>, Arc<KvRpcClient>)>>,
    runtime: OnceCell<Option<Rc<Runtime>>>,
}

tokio::task_local! {
    static CONNECTION: Connection;
    static READ_REACTOR: Handle;
}

/// Own one socket's transport for its entire synchronous service loop. A pool
/// worker may service another socket after this scope has released its state.
pub fn serve<T>(connection: impl FnOnce() -> T) -> T {
    CONNECTION.sync_scope(Connection::default(), connection)
}

/// Drive a read from a synchronous connection worker. Background work
/// spawned by region discovery, PD and retries stays on the shared runtime.
/// Return the unpolled future when the caller needs its usual blocking bridge.
pub fn try_block_on<F: Future>(shared: &Handle, operation: F) -> std::result::Result<F::Output, F> {
    if Handle::try_current().is_ok() {
        return Err(operation);
    }
    let runtime = CONNECTION.try_with(|connection| {
        connection
            .runtime
            .get_or_init(|| {
                Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .ok()
                    .map(Rc::new)
            })
            .clone()
    });
    let Ok(Some(runtime)) = runtime else {
        return Err(operation);
    };
    Ok(runtime
        .block_on(READ_REACTOR.scope(runtime.handle().clone(), on_runtime(shared, operation))))
}

fn read_reactor() -> Option<Handle> {
    READ_REACTOR.try_with(Clone::clone).ok()
}

// Enter the owner only while polling. An enter guard across a suspension
// would also redirect unrelated tasks serviced by the same OS thread.
async fn on_runtime<F: Future>(handle: &Handle, future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    poll_fn(|cx| {
        let _entered = handle.enter();
        future.as_mut().poll(cx)
    })
    .await
}

// Keep the optional transport's dial and batch futures off the ordinary
// dispatch future. Other statements and clients pay no extra heap allocation.
pub(super) fn dispatch_connection_read<'a>(
    shared: &'a KvRpcClient,
    request: &'a dyn Request,
    timeout: Option<Duration>,
    forwarded_host: &'a str,
) -> Option<BoxFuture<'a, Result<Box<dyn Any>>>> {
    let reactor = read_reactor()?;
    if !request.as_any().is::<kvrpcpb::GetRequest>()
        && !request.as_any().is::<crate::proto::coprocessor::Request>()
    {
        return None;
    }
    let connector = shared.read_connector.as_deref()?;
    let identity = shared.connection.read().unwrap().clone()?;
    let timeout = timeout.unwrap_or(shared.timeout);
    Some(Box::pin(async move {
        on_runtime(&reactor, async {
            let (client, remaining) = transport(shared, connector, &identity, timeout).await?;
            client
                .dispatch_uncollapsed(request, Some(remaining), forwarded_host)
                .await
        })
        .await
    }))
}

async fn transport(
    shared: &KvRpcClient,
    connector: &TikvConnect,
    identity: &Arc<ConnectionInfo>,
    timeout: Duration,
) -> Result<(Arc<KvRpcClient>, Duration)> {
    let cached = CONNECTION.with(|state| {
        let pool = state.pool.borrow();
        let (owner, client) = pool.as_ref()?;
        (Arc::ptr_eq(identity, owner) && !client.is_idle()).then(|| Arc::clone(client))
    });
    if let Some(client) = cached {
        return Ok((client, timeout));
    }
    let started = Instant::now();
    let mut client = tokio::time::timeout(timeout, connector.connect_channels(&identity.address))
        .await
        .map_err(|_| {
            shared.wrap_connection_error(Error::GrpcAPI(tonic::Status::deadline_exceeded(
                "connection-local read transport dial timed out",
            )))
        })?
        .map_err(|error| shared.wrap_connection_error(error))?;
    client.event_listener = Arc::clone(&shared.event_listener);
    client.store_metrics = Arc::clone(&shared.store_metrics);
    // A shared pool's invalidation applies to its private transport too. An
    // idle private batch worker must only retire this connection's own pool.
    *client.connection.write().unwrap() = Some(Arc::new(ConnectionInfo {
        address: identity.address.clone(),
        version: identity.version,
        closed: Arc::clone(&identity.closed),
        idle: AtomicBool::new(false),
    }));
    let client = Arc::new(client.with_batch_worker(&connector.batch_config));
    CONNECTION.with(|state| {
        state
            .pool
            .replace(Some((Arc::clone(identity), Arc::clone(&client))))
    });
    Ok((client, timeout.saturating_sub(started.elapsed())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn point_transport_tasks_run_on_the_connection_thread_and_metadata_tasks_on_the_store() {
        let shared = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let caller = thread::current().id();
        let pending = try_block_on(shared.handle(), async { 7 }).err().unwrap();
        assert_eq!(shared.block_on(pending), 7);
        serve(|| {
            for _ in 0..2 {
                let (metadata, transport) = try_block_on(shared.handle(), async {
                    let metadata = tokio::spawn(async { thread::current().id() })
                        .await
                        .unwrap();
                    let reactor = read_reactor().unwrap();
                    let transport = on_runtime(&reactor, async {
                        tokio::spawn(async { thread::current().id() })
                            .await
                            .unwrap()
                    })
                    .await;
                    (metadata, transport)
                })
                .ok()
                .unwrap();
                assert_ne!(metadata, caller);
                assert_eq!(transport, caller);
            }
        });
        assert!(read_reactor().is_none());
        assert!(try_block_on(shared.handle(), async {}).is_err());
    }
    #[test]
    fn point_get_and_coprocessor_receive_batch_responses_on_the_connection_thread() {
        use crate::proto::{kvrpcpb, tikvpb};
        use crate::store::{ClientEventListener, KvConnect};
        use std::sync::Mutex;

        #[derive(Default)]
        struct ReceiverThreads(Mutex<Vec<thread::ThreadId>>);
        impl ClientEventListener for ReceiverThreads {
            fn on_health_feedback(&self, _: &kvrpcpb::HealthFeedback) {
                self.0.lock().unwrap().push(thread::current().id());
            }
        }

        let shared = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (mut server, _) = shared
            .block_on(crate::store::mockserver::start_mock_tikv_service())
            .unwrap();
        server.set_batch_commands_handler(Some(Arc::new(|request| {
            Ok(tikvpb::BatchCommandsResponse {
                request_ids: request.request_ids,
                responses: request
                    .requests
                    .iter()
                    .map(|req| {
                        use tikvpb::batch_commands_request::request::Cmd as In;
                        use tikvpb::batch_commands_response::response::Cmd as Out;
                        let response = match req.cmd.as_ref().unwrap() {
                            In::Get(_) => Out::Get(kvrpcpb::GetResponse {
                                value: b"point-row".to_vec(),
                                ..Default::default()
                            }),
                            In::Coprocessor(_) => {
                                Out::Coprocessor(crate::proto::coprocessor::Response {
                                    data: b"cop-rows".to_vec().into(),
                                    ..Default::default()
                                })
                            }
                            _ => panic!("unexpected request"),
                        };
                        tikvpb::batch_commands_response::Response {
                            cmd: Some(response),
                        }
                    })
                    .collect(),
                health_feedback: Some(kvrpcpb::HealthFeedback::default()),
                ..Default::default()
            })
        })));
        let addr = server.addr().unwrap().to_string();
        let connector = TikvConnect::new(Arc::default(), Duration::from_secs(4), 1024 * 1024);
        let client = shared
            .block_on(connector.connect(&addr))
            .unwrap()
            .with_connection_info(addr, 1);
        let threads = Arc::new(ReceiverThreads::default());
        client.set_event_listener(threads.clone());
        let request = kvrpcpb::GetRequest::default();
        let get = || async {
            let response = client
                .dispatch(&request)
                .await
                .unwrap()
                .downcast::<kvrpcpb::GetResponse>()
                .unwrap();
            assert_eq!(response.value, b"point-row");
        };

        let cop = || async {
            let response = client
                .dispatch(&crate::proto::coprocessor::Request::default())
                .await
                .unwrap()
                .downcast::<crate::proto::coprocessor::Response>()
                .unwrap();
            assert_eq!(&response.data[..], b"cop-rows");
        };
        shared.block_on(get());
        shared.block_on(cop());
        let caller = thread::current().id();
        assert!(threads.0.lock().unwrap().iter().all(|id| *id != caller));
        serve(|| {
            for _ in 0..2 {
                try_block_on(shared.handle(), get()).ok().unwrap();
                try_block_on(shared.handle(), cop()).ok().unwrap();
            }
            assert!(CONNECTION.with(|state| state.pool.borrow().is_some()));
        });
        assert_eq!(threads.0.lock().unwrap()[2..], [caller; 4]);
        serve(|| assert!(CONNECTION.with(|state| state.pool.borrow().is_none())));
        client.close();
        shared.block_on(server.stop()).unwrap();
    }
}
