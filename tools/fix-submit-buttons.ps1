param(
  [string]$Path = ".",
  [switch]$Backup,
  [switch]$WhatIf
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Resolve raiz
$root = (Resolve-Path -Path $Path).Path
Push-Location $root
try {
  $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
  $backupRoot = Join-Path $root ".autofix_submit_$timestamp"
  if ($Backup) { New-Item -ItemType Directory -Force -Path $backupRoot | Out-Null }

  $files = Get-ChildItem -Recurse -File -Filter *.py
  if (-not $files) { Write-Host "Sem ficheiros .py em $root"; exit 0 }

  # padrões
  $rxForm  = [regex]::new('st\s*\.\s*form_submit_button\s*\((?<args>[^)]*)\)', 'IgnoreCase')
  $opts    = [System.Text.RegularExpressions.RegexOptions]::IgnoreCase

  $rxWidth = [regex]::new(@'
\s*,\s*width\s*=\s*(".*?"|'.*?'|[A-Za-z_][A-Za-z0-9_]*)
'@, $opts)

  $rxUCW   = [regex]::new(@'
\s*,\s*use_container_width\s*=\s*(True|False)
'@, $opts)

  $total = 0
  foreach ($f in $files) {
    $orig = Get-Content -Raw -Encoding UTF8 -Path $f.FullName
    $text = $orig

    $me = [System.Text.RegularExpressions.MatchEvaluator]{
      param([System.Text.RegularExpressions.Match]$m)
      $args = $m.Groups['args'].Value

      # remove width=... e use_container_width=... apenas dentro do form_submit_button
      $args = $rxWidth.Replace($args, '')
      $args = $rxUCW.Replace($args, '')

      # limpa vírgula(s) finais
      $args = ($args -replace '\s*,\s*$', '').Trim()

      "st.form_submit_button($args)"
    }

    $newText = $rxForm.Replace($text, $me)

    if ($newText -ne $orig) {
      $total++
      if ($WhatIf) {
        Write-Host "[DRY-RUN] $($f.FullName) seria atualizado."
      } else {
        if ($Backup) {
          $rel  = Resolve-Path -Relative -Path $f.FullName
          $dest = Join-Path $backupRoot $rel
          New-Item -ItemType Directory -Force -Path (Split-Path $dest -Parent) | Out-Null
          Copy-Item $f.FullName $dest -Force
        }
        Set-Content -Path $f.FullName -Value $newText -Encoding UTF8
        Write-Host "[OK] Atualizado: $($f.FullName)"
      }
    }
  }

  if ($WhatIf) {
    Write-Host "`n[DRY-RUN] ficheiros a alterar: $total"
  } else {
    Write-Host "`n✔️ Concluído. Ficheiros alterados: $total"
    if ($Backup) { Write-Host "Backup em: $backupRoot" }
  }
}
finally {
  Pop-Location
}
