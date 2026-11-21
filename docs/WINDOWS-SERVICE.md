# Running Rexyra as a Windows service

As a service, Rexyra starts with Windows before anyone signs in, is restarted if it crashes, and is shut down cleanly when the machine is.

There are two ways to set it up: the [installer](#the-installer), which does everything, or [by hand](#by-hand) from the zip.

## The installer

Download `rexyra-vX.Y.Z-windows-x86_64.msi` from the [releases page](https://github.com/moznhub/rexyra/releases) and run it. Setup asks two things:

- **The folder** to install the program in (`C:\Program Files\Rexyra` unless you change it).
- **The port** to listen on (6379 unless you change it).

It then installs and starts a service called **Rexyra**. The installer is not code-signed, so Windows SmartScreen may ask for confirmation; each release lists a SHA-256 checksum to compare with `Get-FileHash`.

What ends up where:

| | |
|---|---|
| `C:\Program Files\Rexyra\` | The program and its documentation. |
| `C:\ProgramData\Rexyra\rexyra.conf` | The settings. Edit this file and restart the service to change them. |
| `C:\ProgramData\Rexyra\data\` | The snapshot and, if turned on, the append-only file. |
| `C:\ProgramData\Rexyra\logs\rexyra.log` | The log. When it reaches 100 MB it is renamed `rexyra.1.log` and a new one started; five earlier files are kept, the oldest deleted. `logfile-max-size` and `logfile-max-files` in `rexyra.conf` change that. |

The service runs as `LocalService`, an account with few rights, which is allowed to write to `C:\ProgramData\Rexyra` and nowhere else of note. It listens on `127.0.0.1`, so only programs on the same machine can connect until you [say otherwise](#other-machines).

Check that it answers:

```powershell
Test-NetConnection 127.0.0.1 -Port 6379
```

or, with the Redis tools installed, `redis-cli ping`.

### Installing without the windows

For scripted installs, `msiexec` takes the same two choices as properties:

```powershell
msiexec /i rexyra-vX.Y.Z-windows-x86_64.msi /qn PORT=6380 INSTALLFOLDER="D:\Rexyra"
```

### Upgrading and removing

To upgrade, run the installer of the newer version. It stops the service, replaces the program and starts the service again. `rexyra.conf` and the data are not touched.

To remove Rexyra, use **Settings > Apps > Installed apps**. That removes the service and the program. The settings, data and log in `C:\ProgramData\Rexyra` are left in place; delete that folder yourself if you no longer want them.

## Day to day

The service is managed like any other, from the Services console or an administrator PowerShell:

```powershell
Restart-Service Rexyra
```

```powershell
Get-Service Rexyra
```

**Changing a setting.** Edit `C:\ProgramData\Rexyra\rexyra.conf` (as administrator) and restart the service. Every setting is described in the [README](../README.md#configuration). Write a path without quotes, or in single quotes if it contains spaces: inside double quotes a backslash starts an escape sequence, as in `redis.conf`, so `"C:\Rexyra\data"` does not mean what it looks like.

**If the service does not start.** Look at the end of `logs\rexyra.log`. If the mistake is in `rexyra.conf` itself, the server cannot know where its log is, so it writes the reason to `rexyra-startup-error.txt` beside the configuration file instead. Running the server in a terminal shows the same message:

```powershell
& "C:\Program Files\Rexyra\rexyra-server.exe" C:\ProgramData\Rexyra\rexyra.conf
```

## What happens to the data when the service stops

When the service is stopped, or Windows shuts down, Rexyra writes a snapshot if anything changed since the last one, and then exits. The log shows it:

```text
INFO the service manager asked the server to stop
INFO saved snapshot before exit
```

Rexyra keeps telling Windows that it is still saving, so a large data set is not cut off by a stop, and at shutdown Windows waits for it for up to three minutes by default. A power cut or a crash gives no such chance: what changed since the last snapshot is then lost unless the append-only file is on. For data that matters, set `appendonly yes` in `rexyra.conf`; every change is then written to disk within about a second.

**After an internal error.** If Rexyra hits an internal error (a bug), it writes `internal error, stopping so that the server can be restarted` and the details to its log and exits with code 70. The installer sets the service to be restarted after a failure, five seconds later, so it comes back by itself with the data from disk. The installer's test on the build machine checks this with `DEBUG PANIC`.

## Other machines

Out of the box Rexyra listens on `127.0.0.1` only. To accept connections from other machines:

1. In `rexyra.conf`, set `bind 0.0.0.0` (or one specific address of the machine) and a `requirepass` password. Without a password, protected mode refuses every connection that does not come from the machine itself.
2. Restart the service.
3. Open the port in Windows Firewall, to the addresses that need it only:

   ```powershell
   New-NetFirewallRule -DisplayName "Rexyra" -Direction Inbound -Protocol TCP -LocalPort 6379 -RemoteAddress 10.0.0.0/24 -Action Allow
   ```

Rexyra does not support TLS yet. Passwords and data cross the network unencrypted, so expose it to networks you trust only.

## By hand

The same thing without the installer, for the zip download or a build of your own. `rexyra-server.exe --service` is what makes it work: with that option the program talks to the Windows service manager instead of a terminal. Open PowerShell **as administrator**; this example keeps everything in `C:\Rexyra`.

1. Make the folders and copy `rexyra-server.exe` into `C:\Rexyra`:

   ```powershell
   New-Item -ItemType Directory C:\Rexyra\data, C:\Rexyra\logs
   ```

2. Write a configuration file to start from. This creates `rexyra.conf` with these settings and comments on the others, and leaves an existing file alone:

   ```powershell
   C:\Rexyra\rexyra-server.exe --create-config C:\Rexyra\rexyra.conf --port 6379 --dir C:\Rexyra\data --logfile C:\Rexyra\logs\rexyra.log
   ```

   A service needs `dir` and `logfile` to be full paths, because Windows starts it in `C:\Windows\System32`.

3. Let the service account write the data and the log:

   ```powershell
   icacls C:\Rexyra\data /grant "NT AUTHORITY\LocalService:(OI)(CI)M"
   ```

   ```powershell
   icacls C:\Rexyra\logs /grant "NT AUTHORITY\LocalService:(OI)(CI)M"
   ```

4. Register the service and start it. The space after each `=` is required:

   ```powershell
   sc.exe create Rexyra binPath= "C:\Rexyra\rexyra-server.exe --service C:\Rexyra\rexyra.conf" start= auto obj= "NT AUTHORITY\LocalService" DisplayName= "Rexyra"
   ```

   ```powershell
   sc.exe start Rexyra
   ```

To have Windows restart it after a crash:

```powershell
sc.exe failure Rexyra reset= 86400 actions= restart/5000/restart/5000/restart/60000
```

To remove it again:

```powershell
sc.exe stop Rexyra
```

```powershell
sc.exe delete Rexyra
```

If a folder name contains spaces, the path to the program inside `binPath=` needs quotes of its own, which is awkward to type in PowerShell; a folder without spaces avoids it.
