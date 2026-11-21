# Runs the go-redis tests with another go-redis release than the one pinned in
# go.mod, without changing go.mod or go.sum: they are copied to a temporary
# module file that `go get` and `go test` use through -modfile.
#
#   ./test-version.ps1 v9.7.3
#   ./test-version.ps1 v9.7.3 -run TestPubSub -v
#
# Extra arguments go to `go test`. Releases before v9.18 lack client-side
# caching, so the tests are built with -tags goredis_old for them.
# (No param() block, so arguments such as -v reach `go test` untouched.)
if ($args.Count -lt 1) {
    Write-Error 'usage: test-version.ps1 <go-redis version, e.g. v9.7.3> [go test arguments]'
    exit 2
}
$Version = [string]$args[0]
$GoTestArgs = @($args | Select-Object -Skip 1)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

$tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("go-redis-" + $Version + "-" + [guid]::NewGuid().ToString('N').Substring(0, 8))
New-Item -ItemType Directory -Path $tmp | Out-Null
try {
    $modfile = Join-Path $tmp 'go.mod'
    Copy-Item go.mod $modfile
    Copy-Item go.sum (Join-Path $tmp 'go.sum')

    go get "-modfile=$modfile" "github.com/redis/go-redis/v9@$Version"
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    $tags = @()
    if ($Version -match '^v9\.(\d+)\.' -and [int]$Matches[1] -lt 18) {
        $tags = @('-tags', 'goredis_old')
    }
    go test "-modfile=$modfile" @tags -count=1 @GoTestArgs .
    exit $LASTEXITCODE
}
finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}
