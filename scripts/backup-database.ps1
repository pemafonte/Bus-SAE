# Backup lógico da base (motoristas, escalas, serviços, GTFS).
# Requer cliente PostgreSQL (pg_dump) no PATH.
#
# Uso (na raiz do repo, com backend/.env ou $env:DATABASE_URL definido):
#   .\scripts\backup-database.ps1
#   .\scripts\backup-database.ps1 -OutFile "C:\backups\bus_platform.dump"

param(
  [string]$OutFile = ""
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$envPath = Join-Path $root "backend\.env"

if (Test-Path $envPath) {
  Get-Content $envPath | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]*)=(.*)$') {
      $k = $matches[1].Trim()
      $v = $matches[2].Trim().Trim('"')
      [Environment]::SetEnvironmentVariable($k, $v, "Process")
    }
  }
}

$url = $env:DATABASE_URL
if (-not $url) {
  Write-Error "Defina DATABASE_URL ou crie backend\.env com DATABASE_URL=..."
}

if (-not $OutFile) {
  $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
  $OutFile = Join-Path $root "backup-bus_platform-$stamp.dump"
}

& pg_dump --format=custom --file $OutFile $url
if ($LASTEXITCODE -ne 0) {
  Write-Error "pg_dump falhou. Instale as ferramentas de cliente PostgreSQL ou use Docker."
}

Write-Host "Backup guardado em: $OutFile"
