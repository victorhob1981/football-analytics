[CmdletBinding()]
param(
    [string]$CandidateContainer = "football_postgres_v2",
    [string]$RunKey,
    [string]$CompareRunKey,
    [string]$ArtifactRoot = "D:\football-analytics-rebuild"
)

$ErrorActionPreference = "Stop"
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$logRoot = Join-Path $ArtifactRoot "logs"
$fingerprintRoot = Join-Path $ArtifactRoot "fingerprints"
$null = New-Item -ItemType Directory -Force -Path $logRoot, $fingerprintRoot
$reportPath = Join-Path $logRoot "validate-mart-v2-$timestamp.log"

function Invoke-DockerChecked {
    param([string[]]$Arguments)

    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $output = @(& docker @Arguments 2>&1 | ForEach-Object { [string]$_ })
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $previous
    $output | Tee-Object -FilePath $reportPath -Append | Out-Null
    if ($exitCode -ne 0) {
        throw "Docker command failed with exit code ${exitCode}: docker $($Arguments -join ' ')"
    }
    return $output
}

function Invoke-Psql {
    param([string]$Database, [string]$Sql)
    Invoke-DockerChecked @(
        "exec", $CandidateContainer, "psql", "-U", "football", "-d", $Database,
        "-v", "ON_ERROR_STOP=1", "-At", "-F", "|", "-c", $Sql
    )
}

"[$(Get-Date -Format o)] validation_start candidate=$CandidateContainer run_key=$RunKey compare_run_key=$CompareRunKey" | Out-File -FilePath $reportPath -Encoding utf8

$runIdSql = if ($RunKey) {
    "SELECT rebuild_run_id FROM control.rebuild_run WHERE run_key = '$($RunKey.Replace("'", "''"))';"
} else {
    "SELECT rebuild_run_id FROM control.rebuild_run WHERE status='succeeded' ORDER BY rebuild_run_id DESC LIMIT 1;"
}
$runIdOutput = Invoke-Psql -Database "football_dw_v2" -Sql $runIdSql
$runId = ($runIdOutput | Where-Object { $_ -match '^[0-9]+$' } | Select-Object -Last 1)
if (-not $runId) { throw "No rebuild run found" }

$summary = Invoke-Psql -Database "football_dw_v2" -Sql @"
SELECT 'run_id=' || $runId;
SELECT 'matches=' || count(*) FROM mart_v2.fact_match;
SELECT 'published_matches=' || count(*) FROM mart_v2.fact_match WHERE publication_state='published';
SELECT 'quarantined_matches=' || count(*) FROM mart_v2.fact_match WHERE publication_state='quarantined';
SELECT 'pending_sources=' || count(*) FROM mart_v2.match_source WHERE reconciliation_state='pending';
SELECT 'unexplained_reconciliation_scopes=' || count(*)
FROM control.coverage_reconciliation
WHERE rebuild_run_id=$runId AND disposition='pending';
SELECT 'reason_matrix_rows=' || count(*)
FROM control.coverage_delta_reason
WHERE rebuild_run_id=$runId;
"@

$fingerprints = Invoke-Psql -Database "football_dw_v2" -Sql "SELECT object_name || '|' || row_count || '|' || fingerprint FROM control.rebuild_fingerprint WHERE rebuild_run_id=$runId ORDER BY object_name;"
$fingerprintPath = Join-Path $fingerprintRoot "mart-v2-$runId.txt"
$fingerprints | Set-Content -Path $fingerprintPath -Encoding utf8

if ($CompareRunKey) {
    $compareRunOutput = Invoke-Psql -Database "football_dw_v2" -Sql "SELECT rebuild_run_id FROM control.rebuild_run WHERE run_key = '$($CompareRunKey.Replace("'", "''"))';"
    $compareRunId = ($compareRunOutput | Where-Object { $_ -match '^[0-9]+$' } | Select-Object -Last 1)
    if (-not $compareRunId) { throw "Comparison rebuild run not found: $CompareRunKey" }

    $currentFingerprint = (Invoke-Psql -Database "football_dw_v2" -Sql "SELECT coalesce(string_agg(object_name || ':' || row_count || ':' || fingerprint, '|' ORDER BY object_name), '') FROM control.rebuild_fingerprint WHERE rebuild_run_id=$runId;") | Select-Object -Last 1
    $compareFingerprint = (Invoke-Psql -Database "football_dw_v2" -Sql "SELECT coalesce(string_agg(object_name || ':' || row_count || ':' || fingerprint, '|' ORDER BY object_name), '') FROM control.rebuild_fingerprint WHERE rebuild_run_id=$compareRunId;") | Select-Object -Last 1
    if ($currentFingerprint -ne $compareFingerprint) { throw "Fingerprint mismatch: run $runId versus run $compareRunId" }
    "[$(Get-Date -Format o)] fingerprint_comparison=succeeded run=$runId compare_run=$compareRunId" | Tee-Object -FilePath $reportPath -Append
}

"[$(Get-Date -Format o)] validation_complete run_id=$runId fingerprint_file=$fingerprintPath" | Tee-Object -FilePath $reportPath -Append
