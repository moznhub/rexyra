<#
.SYNOPSIS
    Installs the Rexyra MSI silently, checks the service it sets up, and uninstalls it.

.DESCRIPTION
    Meant for a throwaway machine such as a CI runner: it installs a Windows
    service and needs an elevated prompt. It checks that the service runs
    under the account and on the port asked for, that data survives a
    restart of the service, that an upgrade keeps the folder, the settings
    and the data, and that uninstalling removes the service and the program
    but leaves the data.
#>
param(
    # The installer under test, and one of an earlier version to upgrade from.
    [Parameter(Mandatory)] [string] $Msi,
    [Parameter(Mandatory)] [string] $OldMsi,
    [int] $Port = 6400,
    [string] $InstallFolder = 'C:\Rexyra Test\Program'
)

$ErrorActionPreference = 'Stop'
$Msi = (Resolve-Path $Msi).Path
$OldMsi = (Resolve-Path $OldMsi).Path
$dataFolder = Join-Path $env:ProgramData 'Rexyra'
$config = Join-Path $dataFolder 'rexyra.conf'
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

function Invoke-Msi([string[]] $arguments, [string] $log) {
    $process = Start-Process msiexec.exe -ArgumentList ($arguments + @('/qn', '/norestart', '/l*v', $log)) -Wait -PassThru
    if ($process.ExitCode -ne 0) {
        # The lines that say why, rather than the thousands that do not.
        $why = Get-Content $log | Select-String -Pattern 'Return value 3|Error \d+|error|failed' -Context 2, 1 | Select-Object -First 25 | Out-String
        throw "msiexec $($arguments -join ' ') exited with $($process.ExitCode):`n$why"
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

function Get-Diagnostics {
    '--- service'
    (Get-CimInstance Win32_Service -Filter "Name='Rexyra'" | Format-List Name, State, StartMode, StartName, PathName, ExitCode, ServiceSpecificExitCode | Out-String).Trim()
    "--- $dataFolder"
    Get-ChildItem $dataFolder -Recurse -ErrorAction SilentlyContinue | ForEach-Object { $_.FullName }
    (icacls $dataFolder 2>&1 | Out-String).Trim()
    foreach ($file in @($config, (Join-Path $dataFolder 'logs\rexyra.log'), (Join-Path $dataFolder 'rexyra-startup-error.txt'))) {
        if (Test-Path $file) { "--- $file"; Get-Content $file | Where-Object { $_ -notmatch '^\s*(#|$)' } }
    }
}

try {
    Write-Host "== install the earlier version to '$InstallFolder' on port $Port"
    Invoke-Msi @('/i', "`"$OldMsi`"", "PORT=$Port", "INSTALLFOLDER=`"$InstallFolder`"") 'install.log'

    $service = Get-CimInstance Win32_Service -Filter "Name='Rexyra'"
    Check 'the service is installed' ($null -ne $service)
    Check 'it is running' ($service.State -eq 'Running') $service.State
    Check 'it starts automatically' ($service.StartMode -eq 'Auto') $service.StartMode
    Check 'it runs as LocalService' ($service.StartName -match 'Local\s?Service') $service.StartName
    Check 'it runs the installed program in service mode' ($service.PathName -like "*$InstallFolder\rexyra-server.exe*--service*") $service.PathName
    Check 'it is restarted after a crash' ((sc.exe qfailure Rexyra | Out-String) -match 'RESTART') (sc.exe qfailure Rexyra | Out-String)
    Check 'the program is in the chosen folder' (Test-Path (Join-Path $InstallFolder 'rexyra-server.exe'))
    Check 'with the licence and the third-party notices' ((Test-Path (Join-Path $InstallFolder 'LICENSE.txt')) -and (Test-Path (Join-Path $InstallFolder 'THIRD-PARTY-NOTICES.txt')))
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

    Write-Host '== upgrade, naming neither the folder nor the port'
    Invoke-Msi @('/i', "`"$Msi`"") 'upgrade.log'
    $installed = @(Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*' | Where-Object { $_.DisplayName -eq 'Rexyra' })
    Check 'one version is installed afterwards' ($installed.Count -eq 1) "$($installed.Count)"
    Check 'and it is not the earlier one' ($installed[0].DisplayVersion -ne '0.0.1') $installed[0].DisplayVersion
    $service = Get-CimInstance Win32_Service -Filter "Name='Rexyra'"
    Check 'the program stays in the folder chosen before' ($service.PathName -like "*$InstallFolder\rexyra-server.exe*") $service.PathName
    Check 'the service is running' ($service.State -eq 'Running') $service.State
    Check 'the configuration file is kept' ((Get-Content $config) -contains "port $Port")
    Check 'the server answers on its port' (Wait-ForServer)
    Check 'with its data' ((Send-Commands 'GET installer:check') -eq 'kept')

    Write-Host '== uninstall'
    Invoke-Msi @('/x', "`"$Msi`"") 'uninstall.log'
    Check 'the service is gone' ($null -eq (Get-CimInstance Win32_Service -Filter "Name='Rexyra'"))
    Check 'the program is gone' (-not (Test-Path (Join-Path $InstallFolder 'rexyra-server.exe')))
    Check 'the data is left in place' (Test-Path (Join-Path $dataFolder 'data\dump.rdb'))
    Check 'and so is the configuration' (Test-Path $config)
} catch {
    Write-Host "FAIL  $_"
    $failed.Add("FAIL  $_")
}

if ($failed.Count -gt 0) {
    Write-Report (@($failed) + @(Get-Diagnostics))
    throw "$($failed.Count) check(s) failed"
}
Write-Host 'All installer checks passed.'
