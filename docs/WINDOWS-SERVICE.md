# Running Rexyra as a Windows service

As a service, Rexyra starts with Windows before anyone signs in, is restarted if it stops unexpectedly, and is shut down cleanly when the machine is.

Rexyra is a console program and does not register with the Windows service manager itself yet. Pointing `sc create` straight at `rexyra-server.exe` installs a service that fails to start with error 1053. A small **service wrapper** does the registering instead: Windows starts the wrapper, and the wrapper starts, watches and stops Rexyra.

This guide uses [WinSW](https://github.com/winsw/winsw) (MIT licence, one executable, configured by a file you can keep under version control). [NSSM](#using-nssm-instead) works too.

> **How far this has been tested.** What Rexyra itself has to do under a wrapper is tested: starting without a console window, reading a configuration file, logging to a file, and saving and exiting cleanly when the wrapper asks it to stop. The installation steps follow the wrappers' own documentation and have not yet been run end to end by the project. If a step does not work for you, please [open an issue](https://github.com/moznhub/rexyra/issues).

## 1. Lay out a folder

Everything lives in one folder. This guide uses `C:\Rexyra`; if you choose another, a path without spaces saves quoting.

```text
C:\Rexyra\
  rexyra-server.exe      from the Rexyra release
  rexyra.conf            Rexyra's settings           (step 2)
  rexyra-service.exe     WinSW, renamed              (step 3)
  rexyra-service.xml     the service definition      (step 3)
  data\                  snapshot and append-only file
  logs\                  log files
```

Open PowerShell **as administrator** for this and the following steps.

```powershell
New-Item -ItemType Directory C:\Rexyra\data, C:\Rexyra\logs
```

Copy `rexyra-server.exe` from the [release zip](https://github.com/moznhub/rexyra/releases) into `C:\Rexyra`.

## 2. Write the configuration file

Save this as `C:\Rexyra\rexyra.conf` and adjust it. Every setting is described in the [README](../README.md#configuration).

```conf
bind 127.0.0.1
port 6379

# Where the data files go.
dir C:\Rexyra\data

# Log every change to disk, so a stop that is cut short loses at most a second.
appendonly yes

# A limit is worth setting on a shared machine. Without one, Rexyra takes what it is given.
# maxmemory 2gb
# maxmemory-policy allkeys-lru

# Needed before accepting connections from other machines (see "Other machines" below).
# requirepass a-long-random-password
```

**Paths in this file.** Write a path without quotes, as above. Inside double quotes a backslash starts an escape sequence, as in `redis.conf`, so `"C:\Rexyra\data"` does not mean what it looks like. If a path contains spaces, put it in single quotes (`dir 'C:\My Data\rexyra'`) or use forward slashes (`dir "C:/My Data/rexyra"`).

Try it before going further. Start the server by hand, check that it says it is ready, and stop it with Ctrl+C.

```powershell
C:\Rexyra\rexyra-server.exe C:\Rexyra\rexyra.conf
```

## 3. Add the wrapper

Download the 64-bit executable of the latest 2.x version from the [WinSW releases page](https://github.com/winsw/winsw/releases) and save it as `C:\Rexyra\rexyra-service.exe`. WinSW reads the `.xml` file that has the same name as itself, so save the following as `C:\Rexyra\rexyra-service.xml`:

```xml
<service>
  <id>rexyra</id>
  <name>Rexyra</name>
  <description>Rexyra in-memory data server (Redis-compatible)</description>

  <executable>%BASE%\rexyra-server.exe</executable>
  <arguments>"%BASE%\rexyra.conf"</arguments>
  <workingdirectory>%BASE%</workingdirectory>

  <!-- Plain text in the log file, without colour codes. -->
  <env name="NO_COLOR" value="1"/>

  <!-- An account with few rights, rather than the default LocalSystem. -->
  <serviceaccount>
    <domain>NT AUTHORITY</domain>
    <user>LocalService</user>
  </serviceaccount>

  <startmode>Automatic</startmode>
  <onfailure action="restart" delay="10 sec"/>

  <!-- How long Rexyra gets to save and exit when the service is stopped. -->
  <stoptimeout>60sec</stoptimeout>

  <logpath>%BASE%\logs</logpath>
  <log mode="roll-by-size">
    <sizeThreshold>10240</sizeThreshold>
    <keepFiles>8</keepFiles>
  </log>
</service>
```

`%BASE%` is the folder the wrapper is in, so the file keeps working if the folder moves.

## 4. Let the service account write its files

The `LocalService` account can read `C:\Rexyra` like any user, but it must be allowed to write the data and the logs:

```powershell
icacls C:\Rexyra\data /grant "NT AUTHORITY\LocalService:(OI)(CI)M"
```

```powershell
icacls C:\Rexyra\logs /grant "NT AUTHORITY\LocalService:(OI)(CI)M"
```

Leave the rest of the folder as it is, so that the service cannot replace its own executable or configuration.

## 5. Install and start

```powershell
C:\Rexyra\rexyra-service.exe install
```

```powershell
C:\Rexyra\rexyra-service.exe start
```

```powershell
C:\Rexyra\rexyra-service.exe status
```

`status` should print `Started`. From then on the service is called **Rexyra** in the Services console, and `Start-Service rexyra`, `Stop-Service rexyra` and `Restart-Service rexyra` work as for any other service.

Check that it answers:

```powershell
Test-NetConnection 127.0.0.1 -Port 6379
```

or, with the Redis tools installed, `redis-cli ping`.

If it does not start, look in `C:\Rexyra\logs`. `rexyra-service.err.log` holds what Rexyra wrote, including a configuration error or a port that is already taken, and `rexyra-service.wrapper.log` what the wrapper did. A log that cannot be created at all usually means step 4 was skipped.

## What happens to the data when the service stops

When the service is stopped, the wrapper sends Rexyra the same signal as Ctrl+C. Rexyra then writes a snapshot if anything changed since the last one, and exits. The log shows it:

```text
INFO Ctrl+C received, shutting down
INFO saved snapshot before exit
```

Two limits are worth knowing:

- **The stop is given a fixed time.** If Rexyra has not exited after `<stoptimeout>`, the wrapper ends the process, and a snapshot that was still being written is discarded. The previous snapshot stays intact. A large data set on a slow disk can need more than the 60 seconds above.
- **Windows gives services less time when the machine shuts down**, and a power cut or a crash gives none.

With `appendonly yes`, as in the configuration above, neither limit costs more than about the last second of changes, because every change is already in the append-only file when the stop begins. Without it, what changed since the last snapshot can be lost.

## Other machines

Out of the box Rexyra listens on `127.0.0.1` only. To accept connections from other machines:

1. Set `bind 0.0.0.0` (or one specific address of the machine) and a `requirepass` password in `rexyra.conf`. Without a password, protected mode refuses every connection that does not come from the machine itself.
2. Restart the service.
3. Open the port in Windows Firewall, to the addresses that need it only:

   ```powershell
   New-NetFirewallRule -DisplayName "Rexyra" -Direction Inbound -Protocol TCP -LocalPort 6379 -RemoteAddress 10.0.0.0/24 -Action Allow
   ```

Rexyra does not support TLS yet. Passwords and data cross the network unencrypted, so expose it to networks you trust only.

## Upgrading and removing

To upgrade, stop the service, replace `rexyra-server.exe`, and start the service again. The data files stay where they are and are loaded on start.

To remove the service:

```powershell
C:\Rexyra\rexyra-service.exe stop
```

```powershell
C:\Rexyra\rexyra-service.exe uninstall
```

This leaves `C:\Rexyra` and its data in place; delete the folder yourself if you no longer want them.

## Using NSSM instead

[NSSM](https://nssm.cc/) is configured with commands rather than a file (`nssm install Rexyra C:\Rexyra\rexyra-server.exe C:\Rexyra\rexyra.conf`, then `nssm set Rexyra ...`). Its last release is from 2014, with a pre-release from 2017 for Windows 10 and later. If you use it, three settings matter for Rexyra:

- **`AppStopMethodConsole`**: NSSM waits only 1.5 seconds after sending Ctrl+C before it moves on to ending the process. That is too short for a snapshot of more than a small data set. Raise it, for example to `60000` (milliseconds).
- **`AppStderr`**: Rexyra writes its log to standard error; point this at a file such as `C:\Rexyra\logs\rexyra.log`, and set `AppRotateFiles 1` to rotate it.
- **`AppEnvironmentExtra NO_COLOR=1`**: keeps colour codes out of that file.
