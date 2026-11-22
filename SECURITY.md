# Security Policy

## Supported versions

Security fixes are released for the latest 1.x version, as a new patch or minor version; upgrade to get them. There are no maintained branches for older versions, and versions before 1.0 receive no fixes.

## Reporting a vulnerability

Please do **not** open a public issue for a security problem.

Report it privately through GitHub: on the repository's **Security** tab choose **Report a vulnerability**. This opens a private advisory that only the maintainers can see.

Include, as far as you can:

- what an attacker can do (read data, crash the server, bypass authentication, ...);
- the version or commit you tested;
- the configuration that matters (passwords, ACL users, bind address);
- the exact commands or bytes that reproduce it.

You should get an acknowledgement within a week. Once a fix is available the advisory is published, with credit to you unless you prefer otherwise.

## What to know before exposing a server

- **There is no TLS.** Passwords and data cross the network unencrypted. Keep Rexyra on a private network, or put it behind a tunnel.
- **It listens on `127.0.0.1` by default.** With no password set, protected mode refuses connections from other machines; set `requirepass` or ACL users before binding to another address.
- **Lua scripts run inside the server process** with the caller's permissions. Only let trusted clients run `EVAL`, `FUNCTION` and `SCRIPT`; ACL rules can restrict them.
- **Snapshot and log files are trusted input.** Do not load an RDB or append-only file from a source you do not trust.

These are properties of the current design, not vulnerabilities to report; reports about ways around the protections that do exist (authentication, ACL rules, protected mode) are very welcome.
