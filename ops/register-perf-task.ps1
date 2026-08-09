<#
    Register (or remove) the Windows scheduled task that runs a TimeLakeDB
    performance cycle every few hours.

    Runs as the current user, only while that user is logged on — no stored
    password, no SYSTEM privileges. Remove it at any time with -Remove, or
    from Task Scheduler.

        .\ops\register-perf-task.ps1              # every 3 hours from 00:17
        .\ops\register-perf-task.ps1 -Hours 6
        .\ops\register-perf-task.ps1 -Remove
#>
[CmdletBinding()]
param(
    [string]$TaskName = 'TimeLakeDB Perf Cycle',
    [int]$Hours = 3,
    [string]$StartTime = '00:17',
    [switch]$Remove
)

$ErrorActionPreference = 'Stop'
$runner = Join-Path $PSScriptRoot 'run-perf-cycle.ps1'

if ($Remove) {
    schtasks /Delete /TN $TaskName /F
    return
}

if (-not (Test-Path $runner)) { throw "runner not found: $runner" }

# -WindowStyle Hidden so a cycle never steals focus mid-work.
$action = "powershell.exe -NoProfile -NonInteractive -WindowStyle Hidden " +
          "-ExecutionPolicy Bypass -File `"$runner`""

schtasks /Create /TN $TaskName /TR $action /SC HOURLY /MO $Hours /ST $StartTime /F
if ($LASTEXITCODE -ne 0) { throw "schtasks failed with $LASTEXITCODE" }

Write-Output ''
Write-Output "Registered '$TaskName': every $Hours hour(s) from $StartTime."
Write-Output "  logs      : $(Join-Path $PSScriptRoot 'logs')"
Write-Output "  run now   : schtasks /Run /TN `"$TaskName`""
Write-Output "  status    : schtasks /Query /TN `"$TaskName`" /V /FO LIST"
Write-Output "  remove    : .\ops\register-perf-task.ps1 -Remove"
