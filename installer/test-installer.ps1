<#
.SYNOPSIS
    Installs the Rexyra MSI silently, checks the service it sets up, and uninstalls it.

.DESCRIPTION
    Meant for a throwaway machine such as a CI runner: it installs a Windows
    service and needs an elevated prompt. It checks that the service runs
    under the account and on the port asked for, that data survives a
    restart of the service, that an existing configuration file is kept by a
    reinstall, and that uninstalling removes the service and the program but
    leaves the data.
#>
param(
    [Parameter(Mandatory)] [string] $Msi,
    [int] $Port = 6400,
    [string] $InstallFolder = 'C:\Rexyra Test\Program'
)

$ErrorActionPreference = 'Stop'
$Msi = (Resolve-Path $Msi).Path
$dataFolder = Join-Path $env:ProgramData 'Rexyra'
$config = Join-Path $dataFolder 'rexyra.conf'
$failures = 0

function Check([string] $what, [bool] $ok, [string] $detail = '') {
    if ($ok) {
        Write-Host "ok    $what"
    } else {
        Write-Host "FAIL  $what  $detail"
        $script:failures++
    }
}

function Invoke-Msi([string[]] $arguments, [string] $log) {
    $process = Start-Process msiexec.exe -ArgumentList ($arguments + @('/qn', '/norestart', '/l*v', $log)) -Wait -PassThru
    if ($process.ExitCode -ne 0) {
        Write-Host "msiexec $($arguments -join ' ') exited with $($process.ExitCode); the end of its log:"
        Get-Content $log -Tail 120 | Write-Host
        throw "msiexec failed"
    }
}

# Sends commands on one connection and returns the reply lines.
function Send-Commands([string[]] $commands) {
    $client = [System.Net.Sockets.TcpClient]::new()
    $client.Connect('127.0.0.1', $Port)
    try {
        $stream = $client.GetStream()
        $stream.ReadTimeout = 5000
        $reader = [System.IO.StreamReader]::new($stream)
        foreach ($command in $commands) {
            $bytes = [System.Text.Encoding]::ASCII.GetBytes("$command`r`n")
            $stream.Write($bytes, 0, $bytes.Length)
            $line = $reader.ReadLine()
            if ($line -match '^\$(\d+)$') { $line = $reader.ReadLine() }
            $line
        }
    } finally {
        $client.Close()
    }
}

function Wait-ForServer {
    foreach ($attempt in 1..50) {
        try { if ((Send-Commands 'PING') -eq '+PONG') { return $true } } catch { Start-Sleep -Milliseconds 200 }
    }
    $false
}

function Show-Diagnostics {
    Write-Host '--- service'
    Get-CimInstance Win32_Service -Filter "Name='Rexyra'" | Format-List Name, State, StartMode, StartName, PathName, ExitCode, ServiceSpecificExitCode | Out-String | Write-Host
    Write-Host "--- $dataFolder"
    Get-ChildItem $dataFolder -Recurse -ErrorAction SilentlyContinue | ForEach-Object { $_.FullName } | Write-Host
    foreach ($file in @($config, (Join-Path $dataFolder 'logs\rexyra.log'), (Join-Path $dataFolder 'rexyra-startup-error.txt'))) {
        if (Test-Path $file) { Write-Host "--- $file"; Get-Content $file | Write-Host }
    }
}

try {
    Write-Host "== install to '$InstallFolder' on port $Port"
    Invoke-Msi @('/i', "`"$Msi`"", "PORT=$Port", "INSTALLFOLDER=`"$InstallFolder`"") 'install.log'

    $service = Get-CimInstance Win32_Service -Filter "Name='Rexyra'"
    Check 'the service is installed' ($null -ne $service)
    Check 'it is running' ($service.State -eq 'Running') $service.State
    Check 'it starts automatically' ($service.StartMode -eq 'Auto') $service.StartMode
    Check 'it runs as LocalService' ($service.StartName -match 'Local\s?Service') $service.StartName
    Check 'it runs the installed program in service mode' ($service.PathName -like "*$InstallFolder\rexyra-server.exe*--service*") $service.PathName
    Check 'the program is in the chosen folder' (Test-Path (Join-Path $InstallFolder 'rexyra-server.exe'))
    Check 'the configuration file was written' (Test-Path $config)
    Check 'it holds the chosen port' ((Get-Content $config -ErrorAction SilentlyContinue) -contains "port $Port")

    Check 'the server answers on the chosen port' (Wait-ForServer)
    $replies = Send-Commands 'SET installer:check kept', 'RPUSH installer:list a b c'
    Check 'it takes writes' (($replies -join ' ') -eq '+OK :3') ($replies -join ' ')
    Check 'it writes its log' ((Get-Content (Join-Path $dataFolder 'logs\rexyra.log') -Raw -ErrorAction SilentlyContinue) -match 'ready to accept connections')

    Write-Host '== restart the service'
    $watch = [System.Diagnostics.Stopwatch]::StartNew()
    Restart-Service Rexyra
    Write-Host "restarted in $($watch.ElapsedMilliseconds) ms"
    Check 'the server answers again' (Wait-ForServer)
    $replies = Send-Commands 'GET installer:check', 'LLEN installer:list'
    Check 'the data survived the restart' (($replies -join ' ') -eq 'kept :3') ($replies -join ' ')
    Check 'the stop was a clean one' ((Get-Content (Join-Path $dataFolder 'logs\rexyra.log') -Raw) -match 'the service manager asked the server to stop')
    Check 'a snapshot was saved' (Test-Path (Join-Path $dataFolder 'data\dump.rdb'))

    Write-Host '== a mistake in the configuration is reported'
    Stop-Service Rexyra
    $original = Get-Content $config -Raw
    Add-Content $config "`nport not-a-number"
    $started = $true
    try { Start-Service Rexyra -ErrorAction Stop } catch { $started = $false }
    Check 'the service does not start' (-not $started)
    $errorFile = Join-Path $dataFolder 'rexyra-startup-error.txt'
    Check 'the reason is written beside the configuration' ((Get-Content $errorFile -Raw -ErrorAction SilentlyContinue) -match 'config file line \d+')
    Set-Content $config $original -NoNewline
    Start-Service Rexyra
    Check 'it starts again once the file is put right' (Wait-ForServer)
    Check 'and the note is removed' (-not (Test-Path $errorFile))

    Write-Host '== install again over it, asking for another port'
    Invoke-Msi @('/i', "`"$Msi`"", 'PORT=6999', "INSTALLFOLDER=`"$InstallFolder`"") 'reinstall.log'
    Check 'the existing configuration file is kept' ((Get-Content $config) -contains "port $Port")
    Check 'the server still answers on its port' (Wait-ForServer)
    Check 'with its data' ((Send-Commands 'GET installer:check') -eq 'kept')

    Write-Host '== uninstall'
    Invoke-Msi @('/x', "`"$Msi`"") 'uninstall.log'
    Check 'the service is gone' ($null -eq (Get-CimInstance Win32_Service -Filter "Name='Rexyra'"))
    Check 'the program is gone' (-not (Test-Path (Join-Path $InstallFolder 'rexyra-server.exe')))
    Check 'the data is left in place' (Test-Path (Join-Path $dataFolder 'data\dump.rdb'))
    Check 'and so is the configuration' (Test-Path $config)
} catch {
    Write-Host "FAIL  $_"
    $failures++
}

if ($failures -gt 0) {
    Show-Diagnostics
    throw "$failures check(s) failed"
}
Write-Host 'All installer checks passed.'
