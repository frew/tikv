// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use futures::{channel::oneshot, executor::block_on};
use rocksdb::DB;

use crate::RocksSnapshot;

#[derive(Debug)]
struct QueueState {
    // Is there a currently running snapshot?
    is_running: bool,

    // First thread that's the waiting for the currently running snapshot to finish.
    // Will take snapshot when free.
    queued_leader: Option<oneshot::Sender<()>>,

    // Other threads waiting for a new snapshot.
    // Will be sent the snapshot taken by queued_leader.
    queued_followers: Vec<oneshot::Sender<RocksSnapshot>>,
}

// RocksSnapshotQueueManager exists to solve a contention problem in RocksDB.
//
// Getting a RocksDB snapshot is a relatively lightweight operation, but
// it happens underneath the main RocksDB mutex. So in cases where that
// mutex is heavily contended, we don't want to queue up a bunch of snapshot
// requests.
//
// Since the guarantee in getting a snapshot is that its timestamp is
// at or after the time you request it, we can learn from the RocksDB wait group
// concept for committing.
//
// How this works is that if there's no current snapshot running,
// then we just take the snapshot and return it, like previously.
//
// If there is a snapshot running, we first block the first request thread
// and note it as a leader. If additional requests come in, we block those
// threads and note them as followers.
//
// When the current snapshot request is done, if there is an enqueued leader,
// we wake up that thread, it takes ownership of all the enqueued followers,
// requests a snapshot, and send it to all of the followers.
#[derive(Debug)]
pub struct RocksSnapshotQueueManager {
    db: Arc<DB>,
    queue_state: Mutex<QueueState>,
}

impl RocksSnapshotQueueManager {
    pub fn new(db: Arc<DB>) -> Self {
        RocksSnapshotQueueManager {
            db,
            queue_state: Mutex::new(QueueState {
                is_running: false,
                queued_leader: None,
                queued_followers: Vec::new(),
            }),
        }
    }

    pub fn get(&self) -> RocksSnapshot {
        // Get followers for this snapshot request
        let followers = {
            let mut queue_state = self.queue_state.lock().unwrap();
            // First request - start executing
            if !queue_state.is_running {
                queue_state.is_running = true;
                // Run immediately with No followers
                Vec::new()
            // Second concurrent request - we're leader
            } else if queue_state.queued_leader.is_none() {
                let (sender, receiver) = oneshot::channel();
                queue_state.queued_leader = Some(sender);

                // Leave lock and wait for current snapshot request to finish
                std::mem::drop(queue_state);
                block_on(receiver).unwrap();

                // Wake up get followers
                let mut queue_state = self.queue_state.lock().unwrap();
                queue_state.queued_leader = None;
                // Take ownership of all followers at snapshot start time
                queue_state.queued_followers.drain(..).collect()
            // Nth concurrent request - we're follower
            } else {
                let (sender, receiver) = oneshot::channel();
                queue_state.queued_followers.push(sender);

                // Leave lock and wait for leader to return a snapshot
                std::mem::drop(queue_state);
                return block_on(receiver).unwrap();
            }
        };

        // Take snapshot as previously
        let snapshot = RocksSnapshot::new(self.db.clone());

        {
            let mut queue_state = self.queue_state.lock().unwrap();
            // Wake up next leader if exists
            if queue_state.queued_leader.is_some() {
                queue_state.queued_leader.take().unwrap().send(());
            } else {
                queue_state.is_running = false;
            }
        }

        // Send snapshot to followers
        for follower in followers {
            follower.send(snapshot.clone());
        }

        return snapshot;
    }
}
