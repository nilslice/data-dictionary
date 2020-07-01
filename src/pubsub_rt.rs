use std::time;
use std::thread;

use crate::pubsub::Subscriber;
use crate::util;
use crate::db::Db;
use crate::error::{Error, PubsubAction};

use tokio::runtime::Runtime;

pub fn start(mut rt: Runtime, mut db: Db, ms_pull_delay: u64) {
    rt.block_on(async move {
        let gcp_client = Default::default();
        let sub = Subscriber::from_env(&gcp_client).await.unwrap();
        log::info!("subscription '{}' created", sub.name());
        loop {
            thread::sleep(time::Duration::from_millis(ms_pull_delay));

            let resp = match sub.pull().await {
                Ok(resp) => resp,
                Err(e) => {
                    log::error!("failed to pull messages from pubsub, will retry: {}", e);
                    continue;  
                }
            };

            if let Some(mut messages) = resp.received_messages {
                // In the event that multiple partitions are added in a very short period of time, 
                // some partitions may be inserted out of order. This is atypical, since it would 
                // likely mean files were added to cloud storage concurrently within the same 
                // dataset. In any case, a sort is conducted using the `event_time` field of each
                // notification message in the bucket event received from pubsub, which attempts to
                // put inserts in the order they were written to the bucket.
                messages.sort();

                for msg in messages.iter() {
                    match util::handle_payload(
                        &mut db,
                        &msg.message.data,
                        &msg.message.attributes,
                    )
                    .await 
                    {
                        Ok(_) => {
                            if let Err(e) = sub.ack(&msg.ack_id).await {
                                log::error!(
                                    "failed to ack pubsub message with ack_id '{}': {}",
                                    &msg.ack_id,
                                    e
                                )
                            }
                        }
                        Err(e) => {
                            match e {
                                Error::Pubsub(action) => match action {
                                    PubsubAction::IgnoreAndAck => {
                                        if let Err(e) = sub.ack(&msg.ack_id).await {
                                            log::error!(
                                                "failed to ack pubsub message with ack_id '{}': {}",
                                                &msg.ack_id,
                                                e
                                            )
                                        } 
                                    }
                                },
                                _ => log::error!(
                                    "failed to handle event '{:?}' message_id = '{}', will be retried: {}",
                                    msg.message.attributes.event_type,
                                    msg.message.message_id,
                                    e
                                )
                            }
                        },
                    }
                }
            }
        }
    });
}