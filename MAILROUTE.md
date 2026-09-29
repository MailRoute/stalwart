# MailRoute build of Stalwart 0.12.4

This branch is upstream [`v0.12.4`](https://github.com/stalwartlabs/stalwart/releases/tag/v0.12.4)
plus the commits below. It is the source of the Stalwart server MailRoute runs
(AGPL-3.0; offered here as required by section 13).

| Commit | Change |
|---|---|
| POP3: TOP returns the full headers, then n body lines | RFC 1939 `TOP msg n`: headers followed by the first *n* body lines |
