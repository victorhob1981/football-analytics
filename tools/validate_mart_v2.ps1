[CmdletBinding()]
param(
    [string]$CandidateContainer = "football_postgres_v2",
    [string]$RunKey,
    [string]$ArtifactRoot = "D:\football-analytics-rebuild"
)

$ErrorActionPreference = "Stop"
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$logRoot = Join-Path $ArtifactRoot "logs"
$backupRoot = Join-Path $ArtifactRoot "backups"
$null = New-Item -ItemType Directory -Force -Path $logRoot, $backupRoot
$reportPath = Join-Path $logRoot "validate-mart-v2-$timestamp.log"
$backupPath = Join-Path $backupRoot "mart_v2_core-$timestamp.dump"
$containerDump = "/tmp/mart_v2_core-$timestamp.dump"
$restoreDump = "/tmp/mart_v2_core-$timestamp-restore.dump"

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

"[$(Get-Date -Format o)] validation_start candidate=$CandidateContainer run_key=$RunKey" | Out-File -FilePath $reportPath -Encoding utf8

$runIdSql = if ($RunKey) {
    "SELECT rebuild_run_id FROM control.rebuild_run WHERE run_key = '$($RunKey.Replace("'", "''"))';"
} else {
    "SELECT rebuild_run_id FROM control.rebuild_run WHERE status='succeeded' ORDER BY rebuild_run_id DESC LIMIT 1;"
}
$runIdOutput = Invoke-Psql -Database "football_dw_v2" -Sql $runIdSql
$runId = ($runIdOutput | Where-Object { $_ -match '^[0-9]+$' } | Select-Object -Last 1)
if (-not $runId) { throw "No successful rebuild run found" }

$restoreDatabase = "football_dw_v2_restore_$timestamp" -replace '[^a-zA-Z0-9_]', '_'
$startedAt = Get-Date
$restoreIdOutput = Invoke-Psql -Database "football_dw_v2" -Sql @"
INSERT INTO control.restore_validation (rebuild_run_id, backup_path, restore_database, status, log_path, metadata)
VALUES ($runId, '$($backupPath.Replace("'", "''"))', '$restoreDatabase', 'running', '$($reportPath.Replace("'", "''"))', '{"scope":"mart_v2_serving_v2","raw_excluded":true}'::jsonb)
RETURNING restore_validation_id;
"@
$restoreId = ($restoreIdOutput | Where-Object { $_ -match '^[0-9]+$' } | Select-Object -Last 1)

$restoreSucceeded = $false
try {
    Invoke-DockerChecked @(
        "exec", $CandidateContainer, "pg_dump", "-U", "football", "-d", "football_dw_v2",
        "--format=custom", "--no-owner", "--no-privileges", "--schema=control",
        "--schema=mart_v2", "--schema=serving_v2", "--file=$containerDump"
    )
    docker cp "${CandidateContainer}:$containerDump" $backupPath | Tee-Object -FilePath $reportPath -Append
    if ($LASTEXITCODE -ne 0) { throw "docker cp failed" }

    Invoke-DockerChecked @("exec", $CandidateContainer, "createdb", "-U", "football", $restoreDatabase)
    Invoke-DockerChecked @("exec", $CandidateContainer, "psql", "-U", "football", "-d", $restoreDatabase, "-v", "ON_ERROR_STOP=1", "-c", "CREATE EXTENSION IF NOT EXISTS pg_trgm; CREATE EXTENSION IF NOT EXISTS unaccent;")
    Invoke-DockerChecked @("cp", $backupPath, "${CandidateContainer}:$restoreDump")
    Invoke-DockerChecked @("exec", $CandidateContainer, "pg_restore", "-U", "football", "-d", $restoreDatabase, "--no-owner", "--no-privileges", "--exit-on-error", $restoreDump)

    $sourceFingerprint = (Invoke-Psql -Database "football_dw_v2" -Sql "SELECT coalesce(string_agg(object_name || ':' || row_count || ':' || fingerprint, '|' ORDER BY object_name), '') FROM control.rebuild_fingerprint WHERE rebuild_run_id=$runId;") | Select-Object -Last 1
    $restoredFingerprint = (Invoke-Psql -Database $restoreDatabase -Sql "SELECT coalesce(string_agg(object_name || ':' || row_count || ':' || fingerprint, '|' ORDER BY object_name), '') FROM control.rebuild_fingerprint WHERE rebuild_run_id=$runId;") | Select-Object -Last 1
    if ($sourceFingerprint -ne $restoredFingerprint) { throw "restore fingerprint mismatch" }

    $sourceCounts = (Invoke-Psql -Database "football_dw_v2" -Sql "SELECT 'fact_match=' || count(*) FROM mart_v2.fact_match UNION ALL SELECT 'search_document=' || count(*) FROM serving_v2.search_document UNION ALL SELECT 'fact_match_elo_team_stats=' || count(*) FROM mart_v2.fact_match_elo_team_stats;") -join ";"
    $restoredCounts = (Invoke-Psql -Database $restoreDatabase -Sql "SELECT 'fact_match=' || count(*) FROM mart_v2.fact_match UNION ALL SELECT 'search_document=' || count(*) FROM serving_v2.search_document UNION ALL SELECT 'fact_match_elo_team_stats=' || count(*) FROM mart_v2.fact_match_elo_team_stats;") -join ";"
    if ($sourceCounts -ne $restoredCounts) { throw "restore row-count mismatch" }

    Invoke-Psql -Database "football_dw_v2" -Sql @"
UPDATE control.restore_validation
SET status='succeeded', finished_at=now(),
    source_counts=jsonb_build_object('summary', '$($sourceCounts.Replace("'", "''"))'),
    restored_counts=jsonb_build_object('summary', '$($restoredCounts.Replace("'", "''"))')
WHERE restore_validation_id=$restoreId;
"@ | Out-Null
    $restoreSucceeded = $true
    "[$(Get-Date -Format o)] restore_validation_succeeded backup=$backupPath database=$restoreDatabase" | Tee-Object -FilePath $reportPath -Append
}
catch {
    $message = $_.Exception.Message.Replace("'", "''")
    try {
        Invoke-Psql -Database "football_dw_v2" -Sql "UPDATE control.restore_validation SET status='failed', finished_at=now(), metadata=metadata || jsonb_build_object('error', '$message') WHERE restore_validation_id=$restoreId;" | Out-Null
    } catch { }
    throw
}
finally {
    if ($restoreSucceeded -or $restoreDatabase) {
        & docker exec $CandidateContainer dropdb -U football --if-exists $restoreDatabase 2>&1 | Tee-Object -FilePath $reportPath -Append
    }
    & docker exec $CandidateContainer rm -f $containerDump $restoreDump 2>&1 | Tee-Object -FilePath $reportPath -Append
}

"[$(Get-Date -Format o)] validation_complete report=$reportPath" | Tee-Object -FilePath $reportPath -Append
