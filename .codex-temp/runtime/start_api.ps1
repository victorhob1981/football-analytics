Set-Location "C:/Users/Vitinho/Desktop/Projetos/football-analytics"
Get-Content '.env' | ForEach-Object {
  if ($_ -match '^\s*$' -or $_ -match '^\s*#') { return }
  $parts = $_ -split '=', 2
  if ($parts.Length -eq 2) {
    [System.Environment]::SetEnvironmentVariable($parts[0], $parts[1])
  }
}
$env:POSTGRES_HOST = '127.0.0.1'
$env:POSTGRES_PORT = '5432'
$env:FOOTBALL_PG_DSN = "postgresql://$($env:POSTGRES_USER):$($env:POSTGRES_PASSWORD)@127.0.0.1:5432/$($env:POSTGRES_DB)"
python -m uvicorn api.src.main:app --host 127.0.0.1 --port 8010
