# MailRoute build of Stalwart 0.16.24

This branch is upstream [`v0.16.24`](https://github.com/stalwartlabs/stalwart/releases/tag/v0.16.24)
plus the commits below. It is the source of the Stalwart server MailRoute runs
(AGPL-3.0; offered here as required by section 13).

| Commit | Change |
|---|---|
| store: build against FDB 7.3 (`fdb-7_3`) | our FoundationDB clusters run 7.3.x; upstream builds against the 7.4 client API |
| Redis: client certificate (mutual TLS) and custom CA via environment | `STALWART_REDIS_TLS_CERT` / `_KEY` / `_CA`: present a client certificate to Redis/Valkey and Sentinel that require one (`tls-auth-clients yes`), for single, cluster and Sentinel stores. Unset = upstream behaviour. |

Build (community edition, no enterprise features):

    cargo build -p stalwart --release --no-default-features --features "s3,mysql,foundationdb,redis,rocks"
