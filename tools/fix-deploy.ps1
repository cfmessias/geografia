# tools/fix-deploy.ps1
# Corrige deprecações (Streamlit/pandas) para deploy
# Uso:
#   pwsh ./tools/fix-deploy.ps1 -Path . -Backup
#   # ou no Windows PowerShell:
#   powershell -ExecutionPolicy Bypass -File .\tools\fix-deploy.ps1 -Path . -Backup

param(
  [string]$Path = ".",
  [switch]$Backup,
  [switch]$WhatIf
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Normaliza caminho e prepara backup (opcional)
$root = (Resolve-Path -Path $Path).Path
Push-Location $root
try {
  $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
  $backupRoot = Join-Path $root ".autofix_backup_$timestamp"
  if ($Backup) { New-Item -ItemType Directory -Force -Path $backupRoot | Out-Null }

  $files = Get-ChildItem -Path $root -Recurse -Filter *.py -File
  if (-not $files) { Write-Host "Sem ficheiros .py em $root"; return }

  $totalChanged = 0

  foreach ($f in $files) {
    $orig = Get-Content -Raw -Encoding UTF8 -Path $f.FullName
    $text = $orig
    $changed = $false

    # 1) Streamlit: use_container_width -> width
    $text = [regex]::Replace($text, 'use_container_width\s*=\s*True',  'width="stretch"')
    $text = [regex]::Replace($text, 'use_container_width\s*=\s*False', 'width="content"')

    # 2) Pandas: resample("Y") -> ("YE")
    $resampleEval = [System.Text.RegularExpressions.MatchEvaluator]{
      param($m) ".resample($($m.Groups[1].Value)YE$($m.Groups[1].Value))"
    }
    $text = [regex]::Replace($text, '\.resample\(\s*([''"])Y\1\s*\)', $resampleEval)

    # 3) Pandas: freq="Y" -> "YE" (ex.: pd.Grouper(freq="Y"))
    $freqEval = [System.Text.RegularExpressions.MatchEvaluator]{
      param($m) "freq=$($m.Groups[1].Value)YE$($m.Groups[1].Value)"
    }
    $text = [regex]::Replace($text, 'freq\s*=\s*([''"])Y\1', $freqEval)

    # 4) Pandas: garantir observed=False nos groupby(...) sem observed
    $groupbyEval = [System.Text.RegularExpressions.MatchEvaluator]{
      param($m)
      $args = $m.Groups['args'].Value
      if ($args -match 'observed\s*=') { return $m.Value }
      $argsTrim = $args.Trim()
      if ([string]::IsNullOrWhiteSpace($argsTrim)) {
        return ".groupby(observed=False)"
      } else {
        return ".groupby($args, observed=False)"
      }
    }
    $text = [regex]::Replace($text, '\.groupby\((?<args>[^)]*)\)', $groupbyEval)

    if ($text -ne $orig) {
      $changed = $true
      $totalChanged++
      if ($WhatIf) {
        Write-Host "[DRY-RUN] $($f.FullName) seria atualizado."
      } else {
        if ($Backup) {
          # Guarda cópia com mesma estrutura de pastas
          $rel = Resolve-Path -Relative -Path $f.FullName
          $dest = Join-Path $backupRoot $rel
          $destDir = Split-Path $dest -Parent
          New-Item -ItemType Directory -Force -Path $destDir | Out-Null
          Copy-Item -Path $f.FullName -Destination $dest -Force
        }
        Set-Content -Path $f.FullName -Value $text -Encoding UTF8
        Write-Host "[OK] Atualizado: $($f.FullName)"
      }
    }
  }

  if ($WhatIf) {
    Write-Host "`n[DRY-RUN] Ficheiros que seriam alterados: $totalChanged"
  } else {
    Write-Host "`n✔️ Concluído. Ficheiros alterados: $totalChanged"
    if ($Backup) { Write-Host "Backup em: $backupRoot" }
  }
}
finally {
  Pop-Location
}
