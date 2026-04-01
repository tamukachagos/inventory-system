param(
  [int]$Port = 5000
)

$ErrorActionPreference = 'Stop'

function Get-LocalIPv4 {
  $candidate = Get-NetIPAddress -AddressFamily IPv4 |
    Where-Object {
      $_.IPAddress -ne '127.0.0.1' -and
      $_.IPAddress -notlike '169.254.*' -and
      $_.PrefixOrigin -ne 'WellKnown'
    } |
    Select-Object -First 1

  if (-not $candidate) {
    throw 'No active local IPv4 address found.'
  }

  return $candidate.IPAddress
}

$localIp = Get-LocalIPv4
$healthUrl = "http://$localIp`:$Port/health"
$ruleName = 'InventoryAppPort5000'

try {
  $existingRule = Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue
  if (-not $existingRule) {
    New-NetFirewallRule -DisplayName $ruleName -Direction Inbound -Profile Private -Action Allow -Protocol TCP -LocalPort $Port | Out-Null
    Write-Host "Created Windows Firewall rule '$ruleName' for TCP $Port (Private profile)."
  } else {
    Write-Host "Firewall rule '$ruleName' already exists."
  }
} catch {
  Write-Warning "Could not manage firewall rule automatically. Run PowerShell as Administrator if needed."
  Write-Warning $_.Exception.Message
}

Write-Host ""
Write-Host "Use this URL on your iPad (same WiFi):"
Write-Host $healthUrl
Write-Host ""
Write-Host "Starting server on 0.0.0.0:$Port ..."

$env:PORT = "$Port"
node .\server.js
