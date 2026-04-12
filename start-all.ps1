$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$backendPath = Join-Path $root "backend"

function Test-PortInUse {
  param([int]$Port)
  $conn = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
  return $null -ne $conn
}

if (Test-PortInUse -Port 4000) {
  Write-Host "Porta 4000 ja esta em uso. Backend pode ja estar ligado."
} else {
  Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$backendPath'; `$env:Path='C:\Program Files\nodejs;' + `$env:Path; npm run dev"
  Write-Host "Backend a iniciar em http://localhost:4000"
}

if (Test-PortInUse -Port 5500) {
  Write-Host "Porta 5500 ja esta em uso. Frontend pode ja estar ligado."
} else {
  Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$root'; python -m http.server 5500"
  Write-Host "Frontend a iniciar em http://localhost:5500/frontend/index.html"
}

Write-Host ""
Write-Host "Links:"
Write-Host " - Motorista:  http://localhost:5500/frontend/index.html"
Write-Host " - Supervisor: http://localhost:5500/frontend/supervisor.html"
Write-Host " - API:        http://localhost:4000/health"
