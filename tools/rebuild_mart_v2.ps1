[CmdletBinding()]
param(
    [string]$CandidateContainer = "football_postgres_v2",
    [string]$RunKey = "",
    [string]$ArtifactRoot = "D:\football-analytics-rebuild"
)

$ErrorActionPreference = "Stop"
if (-not $RunKey) {
    $RunKey = "mart-v2-local-" + (Get-Date -Format "yyyyMMdd-HHmmss")
}
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\")).Path
$logRoot = Join-Path $ArtifactRoot "logs"
$null = New-Item -ItemType Directory -Force -Path $logRoot
$logPath = Join-Path $logRoot ("rebuild-" + ($RunKey -replace '[^a-zA-Z0-9_.-]', '_') + ".log")

function Invoke-DockerChecked {
    param([string[]]$Arguments)

    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $dockerOutput = @(& docker @Arguments 2>&1 | ForEach-Object { [string]$_ })
    $dockerExitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousErrorActionPreference
    $dockerOutput | Tee-Object -FilePath $logPath -Append
    if ($dockerExitCode -ne 0) {
        throw "Docker command failed with exit code ${dockerExitCode}: docker $($Arguments -join ' ')"
    }
}

$sqlFiles = @(
    "001_source_bridge.sql",
    "002_control_contract.sql",
    "003_provenance_identity.sql",
    "004_core_structure.sql",
    "005_historical_matches.sql",
    "006_match_dedup.sql",
    "007_entities_and_facts.sql",
    "008_serving_v2.sql",
    "009_validation.sql"
)

"[$(Get-Date -Format o)] rebuild_start run_key=$RunKey candidate=$CandidateContainer" | Out-File -FilePath $logPath -Encoding utf8

foreach ($sqlFile in $sqlFiles) {
    $localPath = Join-Path $repoRoot ("db\rebuild_v2\" + $sqlFile)
    if (-not (Test-Path -LiteralPath $localPath -PathType Leaf)) {
        throw "Missing rebuild script: $localPath"
    }

    Invoke-DockerChecked @("cp", $localPath, "${CandidateContainer}:/tmp/${sqlFile}")

    if ($sqlFile -eq "001_source_bridge.sql") {
        $fdwCommand = 'read -r REMOTE_PASSWORD < /run/secrets/postgres_v2_password; export PGPASSWORD="$REMOTE_PASSWORD"; psql -U football -d football_dw_v2 -v ON_ERROR_STOP=1 -v source_password="$REMOTE_PASSWORD" -f /tmp/001_source_bridge.sql'
        Invoke-DockerChecked @("exec", $CandidateContainer, "sh", "-lc", $fdwCommand)
        continue
    }

    Invoke-DockerChecked @(
        "exec", $CandidateContainer, "psql", "-U", "football", "-d", "football_dw_v2",
        "-v", "ON_ERROR_STOP=1", "-v", "rebuild_run_key=$RunKey", "-f", "/tmp/$sqlFile"
    )
}

"[$(Get-Date -Format o)] rebuild_complete run_key=$RunKey log=$logPath" | Tee-Object -FilePath $logPath -Append
