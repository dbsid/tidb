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

//! Go's workerPool for an index lookup. The index scan and its table
//! lookups share the same FIFO and concurrency limit.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

type Job = Box<dyn FnOnce() + Send>;

#[derive(Default)]
struct State {
    queue: VecDeque<Job>,
    workers: usize,
    #[cfg(test)]
    starts: usize,
}

pub(super) struct LookupWorkerPool {
    concurrency: usize,
    state: Mutex<State>,
    idle: Condvar,
}

impl LookupWorkerPool {
    pub(super) fn new(concurrency: usize) -> Arc<Self> {
        Arc::new(Self {
            concurrency: concurrency.max(1),
            state: Mutex::default(),
            idle: Condvar::new(),
        })
    }

    /// Go workerPool.submit. The current worker can take a single queued
    /// job itself. A second queued job can use another worker.
    pub(super) fn submit(
        self: &Arc<Self>,
        lane: &'static str,
        job: impl FnOnce() + Send + 'static,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.queue.push_back(Box::new(job));
        if state.workers != 0 && (state.queue.len() < 2 || state.workers >= self.concurrency) {
            return;
        }
        state.workers += 1;
        #[cfg(test)]
        {
            state.starts += 1;
        }
        drop(state);
        let pool = Arc::clone(self);
        if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tidb_goruntime::worker_pool::spawn_lane_detached(lane, move || pool.run());
        })) {
            // The OS could not start this lane. Existing workers can drain
            // the FIFO. Failure to start the first worker must reach the caller.
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.workers -= 1;
            if state.workers == 0 {
                let abandoned = std::mem::take(&mut state.queue);
                self.idle.notify_all();
                drop(state);
                drop(abandoned);
                std::panic::resume_unwind(panic);
            }
        }
    }

    /// Let the index producer service its queue when its result channel is full.
    pub(super) fn run_one_queued_job(&self) {
        let job = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .queue
            .pop_front();
        if let Some(job) = job {
            Self::run_job(job);
        }
    }

    fn run_job(job: Job) {
        // A panicked task drops its result sender. Continue draining the
        // other tasks so their readers and Close can still finish.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
    }

    fn run(&self) {
        loop {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            let Some(job) = state.queue.pop_front() else {
                state.workers -= 1;
                if state.workers == 0 {
                    self.idle.notify_all();
                }
                return;
            };
            drop(state);
            Self::run_job(job);
        }
    }

    pub(super) fn wait(&self) {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        drop(
            self.idle
                .wait_while(state, |state| state.workers != 0)
                .unwrap_or_else(|p| p.into_inner()),
        );
    }

    #[cfg(test)]
    pub(super) fn starts(&self) -> usize {
        self.state.lock().unwrap().starts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::sync_channel;
    use std::time::Duration;

    #[test]
    fn lookup_workers_drain_one_task_inline_and_add_a_worker_for_a_backlog() {
        let pool = LookupWorkerPool::new(2);
        let (entered, started) = sync_channel(0);
        let (finish, finished) = sync_channel(0);
        pool.submit("idxlookup-index", move || {
            entered.send(()).unwrap();
            finished.recv().unwrap();
        });
        started.recv_timeout(Duration::from_secs(5)).unwrap();

        let (rows, results) = sync_channel(2);
        let first = rows.clone();
        pool.submit("idxlookup-table", move || first.send(1).unwrap());
        assert_eq!(pool.starts(), 1);

        pool.submit("idxlookup-table", move || rows.send(2).unwrap());
        assert_eq!(results.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
        assert_eq!(results.recv_timeout(Duration::from_secs(5)).unwrap(), 2);
        assert_eq!(pool.starts(), 2);
        finish.send(()).unwrap();
        pool.wait();
    }
}
