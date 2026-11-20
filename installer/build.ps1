<#
.SYNOPSIS
    Builds the Rexyra MSI installer.

.DESCRIPTION
    Needs the WiX 5 command-line tool and two of its extensions:

        dotnet tool install --global wix --version 5.0.2
        wix extension add -g WixToolset.UI.wixext/5.0.2
        wix extension add -g WixToolset.Util.wixext/5.0.2

    Python, which collects the licence notices of the libraries built into
    the program (tools\third-party-notices.py), and a release build of the
    server, preferably with the C runtime linked
    in so that the program needs nothing else installed:

        $env:RUSTFLAGS = '-C target-feature=+crt-static'
        cargo build --release -p rexyra-server

.EXAMPLE
    installer\build.ps1 -Version 0.2.0 -Server target\release\rexyra-server.exe -Out rexyra.msi
#>
param(
    # The product version, x.y.z. Windows Installer compares versions to tell an upgrade.
    [Parameter(Mandatory)] [ValidatePattern('^\d+\.\d+\.\d+$')] [string] $Version,
    [Parameter(Mandatory)] [string] $Server,
    [Parameter(Mandatory)] [string] $Out
)

$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$Server = (Resolve-Path $Server).Path

# The notices of the libraries inside the program go into the package.
$notices = Join-Path $root 'target\THIRD-PARTY-NOTICES.txt'
New-Item -ItemType Directory -Force (Split-Path $notices) | Out-Null
python (Join-Path $root 'tools\third-party-notices.py') $notices
if ($LASTEXITCODE -ne 0) { throw "collecting the third-party notices failed with exit code $LASTEXITCODE" }

$arguments = @(
    'build', '-arch', 'x64',
    '-d', "Version=$Version",
    '-d', "Server=$Server",
    '-d', "Root=$root",
    '-d', "Notices=$notices",
    '-ext', 'WixToolset.UI.wixext',
    '-ext', 'WixToolset.Util.wixext',
    '-o', $Out,
    (Join-Path $PSScriptRoot 'Package.wxs'),
    (Join-Path $PSScriptRoot 'UI.wxs')
)
$output = & wix @arguments 2>&1 | ForEach-Object { "$_" }
$output | Write-Host
if ($LASTEXITCODE -ne 0) {
    if ($env:GITHUB_ACTIONS) {
        # Shown on the run's summary page, where the log may not be.
        $text = ($output -join "`n").Replace('%', '%25').Replace("`r", '%0D').Replace("`n", '%0A')
        Write-Host "::error title=The installer did not build::$text"
    }
    throw "wix build failed with exit code $LASTEXITCODE"
}
Write-Host "Built $Out"
