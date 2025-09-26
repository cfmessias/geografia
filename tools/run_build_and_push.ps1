# scripts\run_build_and_push.ps1
#requires -version 5.1
$ErrorActionPreference = 'Stop'

$Repo = "C:\PythonProjects\emStreamlit\Geografia"
$Py   = Join-Path $Repo "tools\exec_build_country_seed.py"

Push-Location $Repo
try {
    Write-Host "▶️  Running $Py ..."
    & python -u $Py
    if ($LASTEXITCODE -ne 0) { throw "Python exited with code $LASTEXITCODE" }

    Write-Host "✅ Python OK. Now git add/commit/push..."
    git add .
    git commit -m "first commit"
    git push -u origin main
}
finally {
    Pop-Location
}
