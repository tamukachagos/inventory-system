param(
  [string]$PgHost = 'localhost',
  [int]$PgPort = 5432,
  [string]$SuperUser = 'postgres',
  [string]$AppUser = 'inventory_app',
  [string]$AppDb = 'inventory_db'
)

$ErrorActionPreference = 'Stop'

function ConvertTo-PlainText {
  param([Security.SecureString]$Secure)

  $ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
  try {
    return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
  } finally {
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr)
  }
}

if (-not (Get-Command psql -ErrorAction SilentlyContinue)) {
  throw 'psql is not in PATH. Install PostgreSQL client tools or add psql to PATH.'
}

$superPwdSecure = Read-Host "PostgreSQL password for superuser '$SuperUser'" -AsSecureString
$appPwdSecure = Read-Host "New password for app user '$AppUser'" -AsSecureString

$superPwd = ConvertTo-PlainText -Secure $superPwdSecure
$appPwd = ConvertTo-PlainText -Secure $appPwdSecure

if ([string]::IsNullOrWhiteSpace($appPwd)) {
  throw 'App user password cannot be empty.'
}

Write-Host 'Creating/updating dedicated DB user and database...'
$env:PGPASSWORD = $superPwd
& psql -h $PgHost -p $PgPort -U $SuperUser -d postgres -v app_user=$AppUser -v app_db=$AppDb -v app_password=$appPwd -f .\create_app_db_user.sql
if ($LASTEXITCODE -ne 0) {
  throw 'Failed to create/update app user or database.'
}

Write-Host 'Applying migration.sql with dedicated app user...'
$env:PGPASSWORD = $appPwd
& psql -h $PgHost -p $PgPort -U $AppUser -d $AppDb -f .\migration.sql
if ($LASTEXITCODE -ne 0) {
  throw 'Failed to apply migration.sql.'
}

if (-not (Test-Path .env)) {
  Copy-Item .env.example .env
}

$envLines = Get-Content .env
$replacements = @{
  'DB_USER=' = "DB_USER=$AppUser"
  'DB_PASSWORD=' = "DB_PASSWORD=$appPwd"
  'DB_HOST=' = "DB_HOST=$PgHost"
  'DB_PORT=' = "DB_PORT=$PgPort"
  'DB_NAME=' = "DB_NAME=$AppDb"
}

for ($i = 0; $i -lt $envLines.Count; $i++) {
  foreach ($prefix in $replacements.Keys) {
    if ($envLines[$i].StartsWith($prefix)) {
      $envLines[$i] = $replacements[$prefix]
      break
    }
  }
}

Set-Content .env $envLines
Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue

Write-Host ''
Write-Host 'Done. Updated .env and applied migration.'
Write-Host 'Start server with: npm start'
Write-Host "Test DB health: http://192.168.1.164:5000/health/db"
