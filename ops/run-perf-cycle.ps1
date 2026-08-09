<#
    One unattended TimelordDB performance cycle.

    Registered as a Windows scheduled task (see register-perf-task.ps1). The
    prompt lives in perf-cycle-prompt.md; what the agent is permitted to do
    lives in ../.claude/settings.local.json. A command outside that allowlist
    ends the run rather than proceeding — the log will say so, and the fix is
    to add the command to the list.
#>
[CmdletBinding()]
param(
    # Skip the run if another cycle is still going.
    [int]$TimeoutMinutes = 100,
    # Delete cycle logs older than this.
    [int]$KeepLogDays = 14
)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $PSScriptRoot 'logs'
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$log = Join-Path $logDir "perf-cycle-$stamp.log"
$lock = Join-Path $logDir '.running'

function Write-Log([string]$msg) {
    $line = "[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg
    Add-Content -Path $log -Value $line -Encoding utf8
    Write-Output $line
}

# One cycle at a time. A stale lock from a killed run is cleared after the
# timeout rather than blocking every future cycle.
if (Test-Path $lock) {
    $age = (Get-Date) - (Get-Item $lock).LastWriteTime
    if ($age.TotalMinutes -lt $TimeoutMinutes) {
        Write-Log "another cycle started $([int]$age.TotalMinutes) min ago is still running - skipping"
        exit 0
    }
    Write-Log "clearing a stale lock ($([int]$age.TotalMinutes) min old)"
    Remove-Item $lock -Force
}

try {
    New-Item -ItemType File -Path $lock -Force | Out-Null
    Set-Location $repo

    $claude = (Get-Command claude -ErrorAction SilentlyContinue).Source
    if (-not $claude) { Write-Log 'claude CLI not on PATH - aborting'; exit 1 }

    $branch = (& git rev-parse --abbrev-ref HEAD).Trim()
    $dirty = (& git status --porcelain | Measure-Object -Line).Lines
    Write-Log "repo $repo on '$branch', $dirty uncommitted change(s)"
    if ($dirty -gt 0) {
        Write-Log 'tree is dirty - skipping this cycle so we never disturb work in progress'
        exit 0
    }

    $promptFile = Join-Path $PSScriptRoot 'perf-cycle-prompt.md'
    $prompt = Get-Content $promptFile -Raw
    Write-Log "starting cycle with $claude"

    # Print mode: no TTY, no interactive prompts. Anything the allowlist does
    # not cover ends the run and lands in this log.
    $prompt | & $claude -p 2>&1 | ForEach-Object {
        Add-Content -Path $log -Value $_ -Encoding utf8
    }
    $code = $LASTEXITCODE
    Write-Log "cycle finished with exit code $code"

    $head = (& git log --oneline -1).Trim()
    Write-Log "HEAD is now: $head"
}
catch {
    Write-Log "cycle failed: $($_.Exception.Message)"
}
finally {
    if (Test-Path $lock) { Remove-Item $lock -Force }
    Get-ChildItem $logDir -Filter 'perf-cycle-*.log' |
        Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$KeepLogDays) } |
        Remove-Item -Force
}
