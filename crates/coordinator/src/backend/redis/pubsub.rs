/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{Msg, PubSubStream};
use futures::StreamExt;
use redis::{AsyncCommands, PushInfo, cluster::ClusterConfig, cluster_async::ClusterConnection};
use std::fmt::Display;
use store::backend::redis::{RedisPool, RedisStore};
use tokio::sync::mpsc::UnboundedReceiver;

pub struct RedisPubSubStream {
    stream: redis::aio::PubSubStream,
}

pub struct RedisClusterPubSubStream {
    _conn: ClusterConnection,
    rx: UnboundedReceiver<PushInfo>,
}

pub(crate) async fn redis_publish(
    redis: &RedisStore,
    topic: &'static str,
    message: Vec<u8>,
) -> trc::Result<()> {
    match &redis.pool {
        RedisPool::Single(pool) => pool
            .get()
            .await
            .map_err(into_error)?
            .as_mut()
            .publish(topic, message)
            .await
            .map_err(into_error),
        RedisPool::Cluster(pool) => pool
            .get()
            .await
            .map_err(into_error)?
            .as_mut()
            .publish(topic, message)
            .await
            .map_err(into_error),
        RedisPool::Sentinel(pool) => pool
            .get()
            .await
            .map_err(into_error)?
            .as_mut()
            .publish(topic, message)
            .await
            .map_err(into_error),
    }
}

pub(crate) async fn redis_subscribe(
    redis: &RedisStore,
    topic: &'static str,
) -> trc::Result<PubSubStream> {
    match &redis.pool {
        RedisPool::Single(pool) => {
            let mut pubsub = pool
                .manager()
                .client
                .get_async_pubsub()
                .await
                .map_err(into_error)?;
            pubsub.subscribe(topic).await.map_err(into_error)?;

            Ok(PubSubStream::Redis(RedisPubSubStream {
                stream: pubsub.into_on_message(),
            }))
        }
        RedisPool::Cluster(pool) => {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

            let mut _conn = pool
                .manager()
                .client
                .get_async_connection_with_config(ClusterConfig::default().set_push_sender(tx))
                .await
                .map_err(into_error)?;

            _conn.subscribe(topic).await.map_err(into_error)?;

            Ok(PubSubStream::RedisCluster(RedisClusterPubSubStream {
                _conn,
                rx,
            }))
        }
        RedisPool::Sentinel(pool) => {
            let client = pool
                .manager()
                .client
                .lock()
                .await
                .async_get_client()
                .await
                .map_err(into_error)?;

            let mut pubsub = client.get_async_pubsub().await.map_err(into_error)?;
            pubsub.subscribe(topic).await.map_err(into_error)?;

            Ok(PubSubStream::Redis(RedisPubSubStream {
                stream: pubsub.into_on_message(),
            }))
        }
    }
}

impl RedisPubSubStream {
    pub async fn next(&mut self) -> Option<Msg> {
        self.stream.next().await.map(Msg::Redis)
    }
}

impl RedisClusterPubSubStream {
    pub async fn next(&mut self) -> Option<Msg> {
        loop {
            if let Some(msg) = redis::Msg::from_push_info(self.rx.recv().await?) {
                return Some(Msg::Redis(msg));
            }
        }
    }
}

#[inline(always)]
fn into_error(err: impl Display) -> trc::Error {
    trc::StoreEvent::RedisError.reason(err)
}

#[cfg(test)]
mod tests {
    //! Mutual-TLS Sentinel pub/sub (STALWART_REDIS_TLS_* env vars). Needs a live
    //! Sentinel + primary with `tls-auth-clients yes`, so it is ignored by default:
    //!
    //!   STALWART_TEST_REDIS_SENTINEL=rediss://127.0.0.1:27390 \
    //!   STALWART_TEST_REDIS_SERVICE=coordtest \
    //!   STALWART_TEST_REDIS_PASSWORD=... STALWART_TEST_REDIS_SENTINEL_PASSWORD=... \
    //!   STALWART_REDIS_TLS_CERT=client.crt STALWART_REDIS_TLS_KEY=client.key \
    //!   STALWART_REDIS_TLS_CA=ca.crt \
    //!   cargo test -p coordinator --features redis -- --ignored redis_sentinel_mtls
    use super::{redis_publish, redis_subscribe};
    use registry::schema::structs::{RedisSentinelStore, SecretKeyOptional, SecretKeyValue};
    use store::InMemoryStore;
    use store::backend::redis::RedisStore;

    #[tokio::test]
    #[ignore]
    async fn redis_sentinel_mtls_pubsub() {
        let env = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("{k} not set"));
        let config = RedisSentinelStore {
            urls: vec![env("STALWART_TEST_REDIS_SENTINEL")].into(),
            service_name: env("STALWART_TEST_REDIS_SERVICE"),
            auth_secret: SecretKeyOptional::Value(SecretKeyValue {
                secret: env("STALWART_TEST_REDIS_PASSWORD"),
            }),
            sentinel_secret: SecretKeyOptional::Value(SecretKeyValue {
                secret: env("STALWART_TEST_REDIS_SENTINEL_PASSWORD"),
            }),
            // requirepass-style servers: no ACL user (the schema default is "stalwart")
            auth_username: None,
            sentinel_username: None,
            pool_max_connections: 4,
            ..Default::default()
        };
        let redis = match RedisStore::open_sentinel(config)
            .await
            .expect("open_sentinel")
        {
            InMemoryStore::Redis(redis) => redis,
            _ => unreachable!(),
        };

        // Pooled connection to the Sentinel-discovered primary.
        redis
            .key_set(b"mtls-test", b"ok", Some(60))
            .await
            .expect("key_set");

        // Pub/sub: a fresh client from the Sentinel builder (the coordinator path).
        let mut stream = redis_subscribe(&redis, "mtls-test-topic")
            .await
            .expect("subscribe");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        redis_publish(&redis, "mtls-test-topic", b"hello".to_vec())
            .await
            .expect("publish");
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("no message within 5 s")
            .expect("stream ended");
        assert_eq!(msg.payload(), b"hello");
    }
}
