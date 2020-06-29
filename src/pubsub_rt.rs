use std::time;
use std::thread;

use crate::pubsub::Subscriber;
use crate::util;
use crate::db::Db;
use crate::error::{Error, PubsubAction};

use tokio::runtime::Runtime;

pub fn start(mut rt: Runtime, mut db: Db, ms_delay: u64) {
    rt.block_on(async move {
        let sub = Subscriber::from_env().await.unwrap();
        log::info!("subscription '{}' created", sub.name());
        loop {
            thread::sleep(time::Duration::from_millis(ms_delay));

            let resp = match sub.pull().await {
                Ok(resp) => resp,
                Err(e) => {
                    log::error!("failed to pull messages from pubsub, will retry: {}", e);
                    continue;  
                }
            };

            if let Some(messages) = resp.received_messages {
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