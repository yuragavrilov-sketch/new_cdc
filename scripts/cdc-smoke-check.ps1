param(
    [string]$BaseUrl = "http://127.0.0.1:5000",
    [switch]$Strict,
    [switch]$SkipDocker
)

$ErrorActionPreference = "Stop"

function Write-Section([string]$Name) {
    Write-Host ""
    Write-Host "== $Name =="
}

function Write-Ok([string]$Message) {
    Write-Host "[OK]   $Message"
}

function Write-Warn([string]$Message) {
    Write-Host "[WARN] $Message"
}

function Write-Fail([string]$Message) {
    Write-Host "[FAIL] $Message"
}

function Read-Json([string]$Url) {
    Invoke-RestMethod -Method Get -Uri $Url -TimeoutSec 8
}

$warnings = New-Object System.Collections.Generic.List[string]
$failures = New-Object System.Collections.Generic.List[string]

Write-Section "Coordinator"
try {
    $health = Read-Json "$BaseUrl/api/health"
    Write-Ok "backend API is reachable at $BaseUrl"
    if ($null -ne $health.db_available) {
        if ($health.db_available) {
            Write-Ok "state DB is available"
        } else {
            $failures.Add("state DB is not available")
            Write-Fail "state DB is not available"
        }
    }
} catch {
    Write-Fail "backend API is not reachable at ${BaseUrl}: $($_.Exception.Message)"
    exit 2
}

if (-not $SkipDocker) {
    Write-Section "Docker compose"
    try {
        $previousErrorActionPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            $composeRaw = @(docker compose ps --format json 2>&1)
        } finally {
            $ErrorActionPreference = $previousErrorActionPreference
        }
        $composeJson = @($composeRaw | Where-Object { $_ -match '^\s*\{' })
        if ($composeJson.Count -eq 0) {
            $warnings.Add("docker compose has no running project containers")
            Write-Warn "no running project containers were reported"
        } else {
            $composeRows = @($composeJson | ForEach-Object { $_ | ConvertFrom-Json })
            $running = @($composeRows | Where-Object { $_.State -eq "running" })
            Write-Ok ("running containers: {0}/{1}" -f $running.Count, $composeRows.Count)
            foreach ($row in $composeRows) {
                Write-Host ("       {0}: {1}" -f $row.Service, $row.State)
            }
        }
    } catch {
        $warnings.Add("docker compose status could not be read")
        Write-Warn "docker compose status could not be read: $($_.Exception.Message)"
    }
}

Write-Section "Universal worker"
try {
    $workers = Read-Json "$BaseUrl/api/workers/status"
    $activeCount = [int]($workers.active_count)
    if ($activeCount -gt 0) {
        Write-Ok "active workers: $activeCount"
    } else {
        $failures.Add("no active universal worker heartbeat")
        Write-Fail "no active universal worker heartbeat"
    }

    if ($workers.cdc_ready) {
        Write-Ok "at least one active worker advertises CDC capability"
    } else {
        $failures.Add("no active worker with CDC capability")
        Write-Fail "no active worker with CDC capability"
    }

    foreach ($worker in @($workers.workers)) {
        $caps = @($worker.capabilities) -join ","
        Write-Host ("       {0} active={1} role={2} caps={3} heartbeat={4}" -f `
            $worker.worker_id, $worker.active, $worker.role, $caps, $worker.last_heartbeat)
    }
} catch {
    $failures.Add("worker status endpoint failed")
    Write-Fail "worker status endpoint failed: $($_.Exception.Message)"
}

Write-Section "External services"
try {
    $metrics = Read-Json "$BaseUrl/api/services/metrics"
    $serviceNames = @("oracle_source", "oracle_target", "kafka", "kafka_connect")
    foreach ($name in $serviceNames) {
        $service = $metrics.$name
        if ($null -eq $service) {
            $warnings.Add("$name metrics missing")
            Write-Warn "$name metrics missing"
            continue
        }
        if ($service.ok) {
            $rtt = if ($null -ne $service.rtt_ms) { "$($service.rtt_ms) ms" } else { "n/a" }
            Write-Ok "$name ok, rtt=$rtt"
        } else {
            $message = if ($service.error) { $service.error } else { "not ok" }
            $failures.Add("$name is not ready: $message")
            Write-Fail "$name is not ready: $message"
        }
    }
} catch {
    $warnings.Add("service metrics endpoint failed")
    Write-Warn "service metrics endpoint failed: $($_.Exception.Message)"
}

Write-Section "Result"
if ($failures.Count -eq 0) {
    Write-Ok "CDC runtime chain is ready enough for a table-add smoke test"
    if ($warnings.Count -gt 0) {
        Write-Warn ("warnings: {0}" -f ($warnings -join "; "))
    }
    exit 0
}

foreach ($failure in $failures) {
    Write-Fail $failure
}

if ($Strict) {
    exit 3
}

Write-Warn "runtime is incomplete; rerun with -Strict to return a failing exit code for these findings"
exit 0
