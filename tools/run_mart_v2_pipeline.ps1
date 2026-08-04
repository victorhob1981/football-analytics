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
    -ArtifactRoot $ArtifactRoot
if ($LASTEXITCODE -ne 0) {
    throw "The mart v2 SQL rebuild failed."
}

$candidatePassword = (& docker exec $CandidateContainer sh -lc "cat /run/secrets/postgres_v2_password" 2>$null | Out-String).Trim()
if (-not $candidatePassword) {
    throw "Candidate database password secret is unavailable."
}

$dbtProject = Join-Path $repoRoot "platform\dbt_v2"
$dbtArguments = @(
    "run", "--rm", "--network", "football-network",
    "-e", "FOOTBALL_PG_HOST=$CandidateContainer",
    "-e", "FOOTBALL_PG_PORT=5432",
    "-e", "FOOTBALL_PG_USER=football",
    "-e", "FOOTBALL_PG_PASSWORD=$candidatePassword",
    "-e", "FOOTBALL_PG_DBNAME=football_dw_v2",
    "-v", "${dbtProject}:/workspace:ro",
    $DbtImage,
    "dbt", "test", "--project-dir", "/workspace", "--profiles-dir", "/workspace", "--target", "local"
)
$dbtOutput = @(& docker @dbtArguments 2>&1 | ForEach-Object { [string]$_ })
$dbtExitCode = $LASTEXITCODE
$dbtOutput | Out-File -FilePath (Join-Path $ArtifactRoot "dbt-v2-$RunKey.log") -Encoding utf8
if ($dbtExitCode -ne 0) {
    throw "The dbt v2 contract tests failed. See $(Join-Path $ArtifactRoot "dbt-v2-$RunKey.log")."
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
