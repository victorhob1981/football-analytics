[CmdletBinding()]
param(
    [string]$CandidateContainer = "football_postgres_v2",
    [string]$RunKey = "",
    [string]$ArtifactRoot = "D:\football-analytics-rebuild",
    [string]$DbtImage = "ghcr.io/dbt-labs/dbt-postgres:1.8.2"
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\")).Path
if (-not (Test-Path -LiteralPath "D:\")) {
    throw "The rebuild artifact drive D: is not available. Refusing to write rebuild artifacts to the SSD."
}
if (-not $RunKey) {
    $RunKey = "mart-v2-local-" + (Get-Date -Format "yyyyMMdd-HHmmss")
}

& (Join-Path $PSScriptRoot "rebuild_mart_v2.ps1") `
    -CandidateContainer $CandidateContainer `
    -RunKey $RunKey `
    -ArtifactRoot $ArtifactRoot `
    -SkipServing
if ($LASTEXITCODE -ne 0) {
    throw "The mart v2 SQL rebuild failed."
}

$candidatePassword = (& docker exec $CandidateContainer sh -lc "cat /run/secrets/postgres_v2_password" 2>$null | Out-String).Trim()
if (-not $candidatePassword) {
    throw "Candidate database password secret is unavailable."
}

$dbtProject = Join-Path $repoRoot "platform\dbt_v2"
$runIdOutput = @(& docker exec $CandidateContainer psql -U football -d football_dw_v2 -At -c "select rebuild_run_id from control.rebuild_run where run_key = '$($RunKey.Replace("'", "''"))';" 2>&1)
$runId = ($runIdOutput | Where-Object { $_ -match '^[0-9]+$' } | Select-Object -Last 1)
if (-not $runId) {
    throw "Could not resolve rebuild_run_id for $RunKey."
}
$dbtArguments = @(
    "run", "--rm", "--network", "football-network",
    "-e", "FOOTBALL_PG_HOST=$CandidateContainer",
    "-e", "FOOTBALL_PG_PORT=5432",
    "-e", "FOOTBALL_PG_USER=football",
    "-e", "FOOTBALL_PG_PASSWORD=$candidatePassword",
    "-e", "FOOTBALL_PG_DBNAME=football_dw_v2",
    "-v", "${dbtProject}:/workspace:ro",
    $DbtImage,
    "run", "--project-dir", "/workspace", "--profiles-dir", "/workspace", "--target", "local",
    "--vars", ('{"rebuild_run_id": ' + $runId + '}'),
    "--target-path", "/tmp/dbt-target", "--log-path", "/tmp/dbt-logs"
)
$dbtOutput = @(& docker @dbtArguments 2>&1 | ForEach-Object { [string]$_ })
$dbtExitCode = $LASTEXITCODE
$dbtOutput | Out-File -FilePath (Join-Path $ArtifactRoot "dbt-v2-run-$RunKey.log") -Encoding utf8
if ($dbtExitCode -ne 0) {
    throw "The dbt v2 materialization failed. See $(Join-Path $ArtifactRoot "dbt-v2-run-$RunKey.log")."
}

$dbtTestArguments = @(
    "run", "--rm", "--network", "football-network",
    "-e", "FOOTBALL_PG_HOST=$CandidateContainer",
    "-e", "FOOTBALL_PG_PORT=5432",
    "-e", "FOOTBALL_PG_USER=football",
    "-e", "FOOTBALL_PG_PASSWORD=$candidatePassword",
    "-e", "FOOTBALL_PG_DBNAME=football_dw_v2",
    "-v", "${dbtProject}:/workspace:ro",
    $DbtImage,
    "test", "--project-dir", "/workspace", "--profiles-dir", "/workspace", "--target", "local",
    "--target-path", "/tmp/dbt-target", "--log-path", "/tmp/dbt-logs"
)
$dbtTestOutput = @(& docker @dbtTestArguments 2>&1 | ForEach-Object { [string]$_ })
$dbtTestExitCode = $LASTEXITCODE
$dbtTestOutput | Out-File -FilePath (Join-Path $ArtifactRoot "dbt-v2-test-$RunKey.log") -Encoding utf8
if ($dbtTestExitCode -ne 0) {
    throw "The dbt v2 contract tests failed. See $(Join-Path $ArtifactRoot "dbt-v2-test-$RunKey.log")."
}

$validationSql = Join-Path $repoRoot "db\rebuild_v2\009_validation.sql"
& docker cp $validationSql "${CandidateContainer}:/tmp/009_validation.sql"
if ($LASTEXITCODE -ne 0) {
    throw "Could not copy the post-dbt validation script."
}
& docker exec $CandidateContainer psql -U football -d football_dw_v2 -v ON_ERROR_STOP=1 -v rebuild_run_key=$RunKey -f /tmp/009_validation.sql
if ($LASTEXITCODE -ne 0) {
    throw "The post-dbt mart v2 validation failed."
}

& (Join-Path $PSScriptRoot "validate_mart_v2.ps1") `
    -CandidateContainer $CandidateContainer `
    -RunKey $RunKey `
    -ArtifactRoot $ArtifactRoot
if ($LASTEXITCODE -ne 0) {
    throw "The final mart v2 validation failed."
}

"[$(Get-Date -Format o)] pipeline_complete run_key=$RunKey artifact_root=$ArtifactRoot" |
    Tee-Object -FilePath (Join-Path $ArtifactRoot "pipeline-$RunKey.log") -Append
