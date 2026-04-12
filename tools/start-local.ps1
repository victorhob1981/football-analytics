[CmdletBinding()]
param(
  [int]$BffPort = 8010,
  [int]$FrontendPort = 3001
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$ApiDir = Join-Path $RepoRoot "api"
$FrontendDir = Join-Path $RepoRoot "frontend"
$RootEnvPath = Join-Path $RepoRoot ".env"
$FrontendEnvPath = Join-Path $FrontendDir ".env.local"
$ArtifactsDir = Join-Path $RepoRoot "artifacts\local-run"
$BffOutLog = Join-Path $ArtifactsDir "start-local.bff.out.log"
$BffErrLog = Join-Path $ArtifactsDir "start-local.bff.err.log"
$FrontendOutLog = Join-Path $ArtifactsDir "start-local.frontend.out.log"
$FrontendErrLog = Join-Path $ArtifactsDir "start-local.frontend.err.log"
$BffPidFile = Join-Path $ArtifactsDir "start-local.bff.pid"
$FrontendPidFile = Join-Path $ArtifactsDir "start-local.frontend.pid"
$RepoRootLower = $RepoRoot.ToLowerInvariant()

function Write-Step {
  param([string]$Message)
  Write-Host "[start-local] $Message"
}

function Assert-Command {
  param(
    [string]$Name,
    [string]$Hint
  )

  try {
    Get-Command $Name -ErrorAction Stop | Out-Null
  } catch {
    throw "$Hint"
  }
}

function Load-DotEnv {
  param([string]$Path)

  if (-not (Test-Path -LiteralPath $Path)) {
    throw "Arquivo obrigatorio nao encontrado: $Path"
  }

  foreach ($line in Get-Content -LiteralPath $Path) {
    $trimmed = $line.Trim()
    if (-not $trimmed -or $trimmed.StartsWith("#")) {
      continue
    }

    if ($trimmed -notmatch '^\s*([^=\s]+)\s*=\s*(.*)\s*$') {
      continue
    }

    $key = $matches[1]
    $value = $matches[2].Trim()

    if (
      ($value.StartsWith('"') -and $value.EndsWith('"')) -or
      ($value.StartsWith("'") -and $value.EndsWith("'"))
    ) {
      $value = $value.Substring(1, $value.Length - 2)
    }

    [Environment]::SetEnvironmentVariable($key, $value, "Process")
  }
}

function Set-DotEnvValue {
  param(
    [string]$Path,
    [string]$Key,
    [string]$Value
  )

  $lines = @()
  if (Test-Path -LiteralPath $Path) {
    $lines = [System.Collections.Generic.List[string]]::new()
    foreach ($existingLine in Get-Content -LiteralPath $Path) {
      $lines.Add($existingLine)
    }
  } else {
    $lines = [System.Collections.Generic.List[string]]::new()
  }

  $pattern = '^\s*' + [regex]::Escape($Key) + '\s*='
  $updated = $false

  for ($index = 0; $index -lt $lines.Count; $index++) {
    if ($lines[$index] -match $pattern) {
      $lines[$index] = "$Key=$Value"
      $updated = $true
      break
    }
  }

  if (-not $updated) {
    $lines.Add("$Key=$Value")
  }

  Set-Content -LiteralPath $Path -Value $lines -Encoding ascii
}

function Get-ListeningProcess {
  param([int]$Port)

  $connection = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue |
    Select-Object -First 1

  if (-not $connection) {
    return $null
  }

  return Get-CimInstance Win32_Process -Filter "ProcessId = $($connection.OwningProcess)" -ErrorAction SilentlyContinue
}

function Test-OwnedProcess {
  param(
    $ProcessInfo,
    [string[]]$OwnedPatterns = @()
  )

  if (-not $ProcessInfo -or -not $ProcessInfo.CommandLine) {
    return $false
  }

  $commandLine = $ProcessInfo.CommandLine.ToLowerInvariant()
  if ($commandLine.Contains($RepoRootLower)) {
    return $true
  }

  foreach ($pattern in $OwnedPatterns) {
    if ($commandLine -like $pattern.ToLowerInvariant()) {
      return $true
    }
  }

  return $false
}

function Stop-OwnedProcess {
  param(
    [string]$Label,
    [int]$Port,
    [string]$PidFile,
    [string[]]$OwnedPatterns = @()
  )

  if (Test-Path -LiteralPath $PidFile) {
    $pidValue = (Get-Content -LiteralPath $PidFile | Select-Object -First 1).Trim()
    if ($pidValue -match '^\d+$') {
      $processFromPid = Get-CimInstance Win32_Process -Filter "ProcessId = $pidValue" -ErrorAction SilentlyContinue
      if (Test-OwnedProcess -ProcessInfo $processFromPid -OwnedPatterns $OwnedPatterns) {
        Stop-Process -Id ([int]$pidValue) -Force -ErrorAction SilentlyContinue
        Write-Step "Processo antigo de $Label encerrado via PID $pidValue."
      }
    }
    Remove-Item -LiteralPath $PidFile -Force -ErrorAction SilentlyContinue
  }

  $processOnPort = Get-ListeningProcess -Port $Port
  if (-not $processOnPort) {
    return
  }

  if (Test-OwnedProcess -ProcessInfo $processOnPort -OwnedPatterns $OwnedPatterns) {
    Stop-Process -Id $processOnPort.ProcessId -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
    Write-Step "Processo antigo de $Label encerrado na porta $Port."
    return
  }

  throw "Porta $Port ja esta em uso por processo externo ao repo: PID $($processOnPort.ProcessId) [$($processOnPort.Name)]."
}

function Clear-FrontendCache {
  $nextDir = Join-Path $FrontendDir ".next"
  if (-not (Test-Path -LiteralPath $nextDir)) {
    return
  }

  for ($attempt = 1; $attempt -le 3; $attempt++) {
    try {
      Remove-Item -LiteralPath $nextDir -Recurse -Force
      Write-Step "Cache .next limpo para evitar runtime stale."
      return
    } catch {
      if ($attempt -eq 3) {
        throw
      }
      Start-Sleep -Seconds 2
    }
  }
}

function Wait-HttpReady {
  param(
    [string]$Label,
    [string]$Uri,
    [int]$TimeoutSec = 120
  )

  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ((Get-Date) -lt $deadline) {
    try {
      $response = Invoke-WebRequest -UseBasicParsing $Uri -TimeoutSec 15
      if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) {
        return $response
      }
    } catch {
    }

    Start-Sleep -Seconds 2
  }

  throw "$Label nao respondeu em ate $TimeoutSec s: $Uri"
}

function Wait-PostgresReady {
  param([int]$TimeoutSec = 120)

  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ((Get-Date) -lt $deadline) {
    try {
      docker compose exec -T postgres psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "select 1;" |
        Out-Null
      return
    } catch {
    }

    Start-Sleep -Seconds 2
  }

  throw "Postgres nao respondeu apos $TimeoutSec s."
}

function Test-BackendDependencies {
  & python -c "import fastapi, uvicorn, psycopg, psycopg_pool" 2>$null | Out-Null
}

function Test-FrontendDependencies {
  $checkScript = @"
const path = require('node:path');
const postcssPkg = require.resolve('postcss/package.json');
const postcssDir = path.dirname(postcssPkg);
require.resolve('next/package.json');
require.resolve('picocolors/package.json', { paths: [postcssDir] });
"@

  Push-Location $FrontendDir
  try {
    & node -e $checkScript 2>$null | Out-Null
  } finally {
    Pop-Location
  }
}

function Get-PnpmCommandPath {
  $cmdCandidate = Join-Path $env:APPDATA "npm\pnpm.cmd"
  if (Test-Path -LiteralPath $cmdCandidate) {
    return $cmdCandidate
  }

  $command = Get-Command pnpm -ErrorAction Stop
  return $command.Source
}

Assert-Command -Name "docker" -Hint "Docker nao encontrado no PATH. Instale o Docker Desktop e tente novamente."
Assert-Command -Name "python" -Hint "Python nao encontrado no PATH. Ative o ambiente correto e tente novamente."
Assert-Command -Name "node" -Hint "Node.js nao encontrado no PATH. Instale Node.js 20+ e tente novamente."
Assert-Command -Name "pnpm" -Hint "pnpm nao encontrado no PATH. Instale pnpm e tente novamente."

if (-not (Test-Path -LiteralPath $ApiDir)) {
  throw "Diretorio da API nao encontrado: $ApiDir"
}

if (-not (Test-Path -LiteralPath $FrontendDir)) {
  throw "Diretorio do frontend nao encontrado: $FrontendDir"
}

New-Item -ItemType Directory -Force -Path $ArtifactsDir | Out-Null

Load-DotEnv -Path $RootEnvPath

if (-not $env:POSTGRES_USER) {
  [Environment]::SetEnvironmentVariable("POSTGRES_USER", "football", "Process")
}
if (-not $env:POSTGRES_PASSWORD) {
  [Environment]::SetEnvironmentVariable("POSTGRES_PASSWORD", "football", "Process")
}
if (-not $env:POSTGRES_DB) {
  [Environment]::SetEnvironmentVariable("POSTGRES_DB", "football_dw", "Process")
}

[Environment]::SetEnvironmentVariable("POSTGRES_HOST", "127.0.0.1", "Process")
[Environment]::SetEnvironmentVariable("POSTGRES_PORT", "5432", "Process")
[Environment]::SetEnvironmentVariable(
  "FOOTBALL_PG_DSN",
  "postgresql://$($env:POSTGRES_USER):$($env:POSTGRES_PASSWORD)@127.0.0.1:5432/$($env:POSTGRES_DB)",
  "Process"
)

Set-DotEnvValue -Path $FrontendEnvPath -Key "NEXT_PUBLIC_BFF_BASE_URL" -Value "http://127.0.0.1:$BffPort"
Set-DotEnvValue -Path $FrontendEnvPath -Key "NEXT_PUBLIC_APP_ENV" -Value "local"
[Environment]::SetEnvironmentVariable("NEXT_PUBLIC_BFF_BASE_URL", "http://127.0.0.1:$BffPort", "Process")

Write-Step "Subindo stack Docker."
Push-Location $RepoRoot
try {
  docker compose up -d
} finally {
  Pop-Location
}

Write-Step "Validando Postgres."
Push-Location $RepoRoot
try {
  Wait-PostgresReady
} finally {
  Pop-Location
}

try {
  Test-BackendDependencies
  Write-Step "Dependencias da API ja estao disponiveis."
} catch {
  Write-Step "Instalando dependencias da API."
  python -m pip install --disable-pip-version-check -r (Join-Path $ApiDir "requirements.txt")
}

try {
  Test-FrontendDependencies
  Write-Step "Dependencias do frontend ja estao consistentes."
} catch {
  Write-Step "Reparando dependencias do frontend com pnpm install --force."
  Push-Location $FrontendDir
  try {
    & (Get-PnpmCommandPath) install --force
  } finally {
    Pop-Location
  }
}

Stop-OwnedProcess -Label "BFF" -Port $BffPort -PidFile $BffPidFile -OwnedPatterns @("*uvicorn*src.main:app*")
Stop-OwnedProcess -Label "frontend" -Port $FrontendPort -PidFile $FrontendPidFile -OwnedPatterns @("*next*start-server.js*", "*next*dev*-p*$FrontendPort*")
Clear-FrontendCache

if (Test-Path -LiteralPath $BffOutLog) {
  Remove-Item -LiteralPath $BffOutLog -Force
}
if (Test-Path -LiteralPath $BffErrLog) {
  Remove-Item -LiteralPath $BffErrLog -Force
}
if (Test-Path -LiteralPath $FrontendOutLog) {
  Remove-Item -LiteralPath $FrontendOutLog -Force
}
if (Test-Path -LiteralPath $FrontendErrLog) {
  Remove-Item -LiteralPath $FrontendErrLog -Force
}

Write-Step "Subindo BFF em http://127.0.0.1:$BffPort."
$bffProcess = Start-Process `
  -FilePath (Get-Command python).Source `
  -ArgumentList @("-m", "uvicorn", "src.main:app", "--host", "127.0.0.1", "--port", "$BffPort") `
  -WorkingDirectory $ApiDir `
  -RedirectStandardOutput $BffOutLog `
  -RedirectStandardError $BffErrLog `
  -PassThru
$bffProcess.Id | Set-Content -LiteralPath $BffPidFile -Encoding ascii

Write-Step "Aguardando health do BFF."
$null = Wait-HttpReady -Label "BFF" -Uri "http://127.0.0.1:$BffPort/health"

Write-Step "Subindo frontend em http://127.0.0.1:$FrontendPort."
$frontendProcess = Start-Process `
  -FilePath (Get-PnpmCommandPath) `
  -ArgumentList @("exec", "next", "dev", "-p", "$FrontendPort") `
  -WorkingDirectory $FrontendDir `
  -RedirectStandardOutput $FrontendOutLog `
  -RedirectStandardError $FrontendErrLog `
  -PassThru
$frontendProcess.Id | Set-Content -LiteralPath $FrontendPidFile -Encoding ascii

Write-Step "Aguardando frontend."
$null = Wait-HttpReady -Label "frontend" -Uri "http://127.0.0.1:$FrontendPort"

Write-Step "Validando rota da API com banco."
$apiProbe = Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$BffPort/api/v1/matches?competition=sudamericana&season=2024&pageSize=1" -TimeoutSec 30

Write-Host ""
Write-Host "Ambiente local pronto."
Write-Host "Airflow : http://127.0.0.1:8080"
Write-Host "Metabase: http://127.0.0.1:3000"
Write-Host "BFF     : http://127.0.0.1:$BffPort"
Write-Host "Frontend: http://127.0.0.1:$FrontendPort"
Write-Host ""
Write-Host "Evidencia objetiva:"
Write-Host "- Postgres validado via docker compose exec -T postgres psql -c 'select 1;'"
Write-Host "- BFF /health = 200"
Write-Host "- BFF /api/v1/matches... = $($apiProbe.StatusCode)"
Write-Host "- Frontend / = 200"
Write-Host ""
Write-Host "Logs:"
Write-Host "- $BffOutLog"
Write-Host "- $BffErrLog"
Write-Host "- $FrontendOutLog"
Write-Host "- $FrontendErrLog"
