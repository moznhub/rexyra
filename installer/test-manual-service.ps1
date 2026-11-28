<#
.SYNOPSIS
    Sets Rexyra up as a service by hand, the way docs/WINDOWS-SERVICE.md describes, and checks it.

.DESCRIPTION
    Meant for a throwaway machine such as a CI runner: it registers a Windows
    service and needs an elevated prompt. The commands are the ones in the
    guide, so that the guide is known to work.
#>
param(
    [Parameter(Mandatory)] [string] $Server,
    [int] $Port = 6401
)

$ErrorActionPreference = 'Stop'
$failed = [System.Collections.Generic.List[string]]::new()

function Check([string] $what, [bool] $ok, [string] $detail = '') {
    if ($ok) {
        Write-Host "ok    $what"
    } else {
        Write-Host "FAIL  $what  $detail"
        $script:failed.Add("FAIL  $what  $detail")
    }
}

# What went wrong, for the end of the output and, on GitHub Actions, as an
# annotation on the run.
function Write-Report([string[]] $lines) {
    $lines | Write-Host
    if ($env:GITHUB_ACTIONS) {
        $text = ($lines -join "`n")
        if ($text.Length -gt 60000) { $text = $text.Substring(0, 60000) }
        $text = $text.Replace('%', '%25').Replace("`r", '%0D').Replace("`n", '%0A')
        Write-Host "::error title=Service checks failed::$text"
    }
}

function Send-Command([string] $command) {
    $client = [System.Net.Sockets.TcpClient]::new()
    $client.Connect('127.0.0.1', $Port)
    try {
        $stream = $client.GetStream()
        $stream.ReadTimeout = 5000
        $reader = [System.IO.StreamReader]::new($stream)
        $bytes = [System.Text.Encoding]::ASCII.GetBytes("$command`r`n")
        $stream.Write($bytes, 0, $bytes.Length)
        $line = $reader.ReadLine()
        if ($line -match '^\$(\d+)$') { $line = $reader.ReadLine() }
        $line
    } finally {
        $client.Close()
    }
}

function Wait-ForServer {
    foreach ($attempt in 1..50) {
        try { if ((Send-Command 'PING') -eq '+PONG') { return $true } } catch { Start-Sleep -Milliseconds 200 }
    }
    $false
}

try {
    # Step 2
    New-Item -ItemType Directory C:\Rexyra\data, C:\Rexyra\logs | Out-Null
    Copy-Item $Server C:\Rexyra\rexyra-server.exe

    # Step 3
    C:\Rexyra\rexyra-server.exe --create-config C:\Rexyra\rexyra.conf --port $Port --dir C:\Rexyra\data --logfile C:\Rexyra\logs\rexyra.log
    Check 'the configuration file was written' ((Get-Content C:\Rexyra\rexyra.conf) -contains "port $Port")

    # Step 4
    icacls C:\Rexyra\data /grant "NT AUTHORITY\LocalService:(OI)(CI)M" | Out-Null
    icacls C:\Rexyra\logs /grant "NT AUTHORITY\LocalService:(OI)(CI)M" | Out-Null

    # Steps 5 and 7 (6, restarting after a crash, follows)
    sc.exe create Rexyra binPath= "C:\Rexyra\rexyra-server.exe --service C:\Rexyra\rexyra.conf" start= auto obj= "NT AUTHORITY\LocalService" DisplayName= "Rexyra"
    Check 'sc.exe create succeeded' ($LASTEXITCODE -eq 0) "exit code $LASTEXITCODE"
    sc.exe start Rexyra | Out-Null
    Check 'sc.exe start succeeded' ($LASTEXITCODE -eq 0) "exit code $LASTEXITCODE"

    $service = Get-CimInstance Win32_Service -Filter "Name='Rexyra'"
    Check 'the service runs as LocalService' ($service.StartName -match 'Local\s?Service') $service.StartName
    Check 'the server answers' (Wait-ForServer)
    Check 'it takes a write' ((Send-Command 'SET manual:check kept') -eq '+OK')

    sc.exe failure Rexyra reset= 86400 actions= restart/5000/restart/5000/restart/60000 | Out-Null
    Check 'sc.exe failure succeeded' ($LASTEXITCODE -eq 0) "exit code $LASTEXITCODE"

    Restart-Service Rexyra
    Check 'the server answers after a restart' (Wait-ForServer)
    Check 'the data survived it' ((Send-Command 'GET manual:check') -eq 'kept')
    $log = Get-Content C:\Rexyra\logs\rexyra.log -Raw
    Check 'the log shows the clean stop' ($log -match 'the service manager asked the server to stop' -and $log -match 'saved snapshot before exit')

    # A crash: the service manager should bring the server back.
    Stop-Process -Id (Get-CimInstance Win32_Service -Filter "Name='Rexyra'").ProcessId -Force
    Start-Sleep -Seconds 8
    Check 'the service is restarted after a crash' (Wait-ForServer)

    sc.exe stop Rexyra | Out-Null
    foreach ($attempt in 1..50) {
        if ((Get-Service Rexyra).Status -eq 'Stopped') { break }
        Start-Sleep -Milliseconds 200
    }
    sc.exe delete Rexyra | Out-Null
    Check 'sc.exe delete succeeded' ($LASTEXITCODE -eq 0) "exit code $LASTEXITCODE"
} catch {
    Write-Host "FAIL  $_"
    $failed.Add("FAIL  $_")
}

if ($failed.Count -gt 0) {
    $report = @($failed)
    $report += (Get-CimInstance Win32_Service -Filter "Name='Rexyra'" | Format-List Name, State, StartName, PathName, ExitCode, ServiceSpecificExitCode | Out-String).Trim()
    foreach ($file in 'C:\Rexyra\rexyra.conf', 'C:\Rexyra\logs\rexyra.log', 'C:\Rexyra\rexyra-startup-error.txt') {
        if (Test-Path $file) { $report += "--- $file"; $report += Get-Content $file | Where-Object { $_ -notmatch '^\s*(#|$)' } }
    }
    Write-Report $report
    throw "$($failed.Count) check(s) failed"
}
Write-Host 'All checks of the manual set-up passed.'
