$root = 'c:\Users\HP\OneDrive\Desktop\PROJECTS\Segmento_Collector'

# -------------------------------------------------------
# 1. Fix all backend files with:  DB = "identity.db"
# -------------------------------------------------------
$backendFiles = Get-ChildItem -Path "$root\backend" -Recurse -Include '*.py' |
    Where-Object { (Get-Content $_.FullName -Raw -Encoding UTF8) -match 'DB = "identity\.db"' }

foreach ($file in $backendFiles) {
    $content = Get-Content $file.FullName -Raw -Encoding UTF8

    # Ensure 'import os' is present (add after 'import sqlite3' if missing)
    if ($content -notmatch '(?m)^import os') {
        $content = $content -replace '(import sqlite3)', "`$1`nimport os"
    }

    # Replace the bare DB constant
    $content = $content -replace 'DB = "identity\.db"', 'DB = os.getenv("DB_PATH", "/tmp/identity.db")'

    Set-Content $file.FullName $content -Encoding UTF8 -NoNewline
    Write-Host "Fixed (DB constant): $($file.FullName)"
}

# -------------------------------------------------------
# 2. Fix scheduler.py  DB_PATH = os.path.join(PROJECT_ROOT, "identity.db")
# -------------------------------------------------------
$schedulerFile = "$root\backend\scheduler\scheduler.py"
$content = Get-Content $schedulerFile -Raw -Encoding UTF8
$content = $content -replace 'DB_PATH = os\.path\.join\(PROJECT_ROOT, "identity\.db"\)', 'DB_PATH = os.getenv("DB_PATH", "/tmp/identity.db")'
Set-Content $schedulerFile $content -Encoding UTF8 -NoNewline
Write-Host "Fixed (scheduler.py DB_PATH): $schedulerFile"

# -------------------------------------------------------
# 3. Fix github.py inline  sqlite3.connect("identity.db")  -> use DB constant
# -------------------------------------------------------
$githubFile = "$root\backend\connectors\github.py"
$content = Get-Content $githubFile -Raw -Encoding UTF8
$content = $content -replace 'sqlite3\.connect\("identity\.db"\)', 'sqlite3.connect(DB)'
Set-Content $githubFile $content -Encoding UTF8 -NoNewline
Write-Host "Fixed (github.py inline connect): $githubFile"

# -------------------------------------------------------
# 4. Fix google_gcs.py  DB = os.path.join(BASE_DIR, "identity.db")
# -------------------------------------------------------
$gcsFile = "$root\backend\connectors\google_gcs.py"
$content = Get-Content $gcsFile -Raw -Encoding UTF8
$content = $content -replace 'DB = os\.path\.join\(BASE_DIR, "identity\.db"\)', 'DB = os.getenv("DB_PATH", "/tmp/identity.db")'
Set-Content $gcsFile $content -Encoding UTF8 -NoNewline
Write-Host "Fixed (google_gcs.py): $gcsFile"

# -------------------------------------------------------
# 5. Fix frontend/ui_server.py
#    DB_PATH = os.path.join(BASE_DIR, "..", "identity.db")
#    + all hardcoded sqlite3.connect("../identity.db")
# -------------------------------------------------------
$uiFile = "$root\frontend\ui_server.py"
$content = Get-Content $uiFile -Raw -Encoding UTF8
$content = $content -replace 'DB_PATH = os\.path\.join\(BASE_DIR, "\.\.", "identity\.db"\)', 'DB_PATH = os.getenv("DB_PATH", "/tmp/identity.db")'
# Fix  sqlite3.connect("../identity.db")  (both forward-slash variants)
$content = $content -replace 'sqlite3\.connect\("\.\./identity\.db"\)', 'sqlite3.connect(DB_PATH)'
$content = $content -replace "sqlite3\.connect\('\.\.\/identity\.db'\)", 'sqlite3.connect(DB_PATH)'
Set-Content $uiFile $content -Encoding UTF8 -NoNewline
Write-Host "Fixed (ui_server.py): $uiFile"

Write-Host ""
Write-Host "=== ALL DONE ==="
