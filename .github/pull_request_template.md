## What this changes

<!-- One or two sentences. Link the issue it closes, if any: "Closes owner/repo#number". -->

## How it was checked

<!-- Tests added or changed, and what you compared with Redis (version and result). -->

## Checklist

- [ ] `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace` pass
- [ ] New or changed behaviour has a test that fails without this change
- [ ] Replies and error messages were compared with Redis, where a client can see them
- [ ] The README command list or `docs/COMPATIBILITY.md` is updated if behaviour visible to users changed
- [ ] No code was copied from Redis
