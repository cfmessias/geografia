# watch_python.ps1 — lista processos Python que correm .py, com início e duração

function Get-PyFromCmd([string]$cmd) {
  if (-not $cmd) { return '' }
  # último .py entre aspas
  $m = [regex]::Matches($cmd, '(?i)"([^"]+?\.py)"')
  if ($m.Count -gt 0) { return $m[$m.Count-1].Groups[1].Value }
  # último .py sem aspas
  $m2 = [regex]::Matches($cmd, '(?i)(?<=\s|^)(\S+?\.py)(?=\s|$)')
  if ($m2.Count -gt 0) { return $m2[$m2.Count-1].Groups[1].Value }
  return ''
}

function Try-ConvertDmtf([string]$dmtf) {
  try {
    if ([string]::IsNullOrWhiteSpace($dmtf)) { return $null }
    return [Management.ManagementDateTimeConverter]::ToDateTime($dmtf)
  } catch { return $null }
}

# Obter processos python/python3/pythonw com .py na linha de comandos
$rows = @()
Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
  Where-Object { $_.Name -match '^python(3)?w?\.exe$' -and $_.CommandLine -match '\.py' } |
  ForEach-Object {
    $procId = $_.ProcessId
    $cmd    = $_.CommandLine
    $script = Get-PyFromCmd $cmd

    # 1) tentar StartTime via Get-Process
    $start = $null
    try {
      $gp = Get-Process -Id $procId -ErrorAction Stop
      $start = $gp.StartTime
    } catch {
      # 2) fallback: CreationDate do WMI
      $start = Try-ConvertDmtf $_.CreationDate
    }

    $dur = $null
    if ($start) { $dur = New-TimeSpan -Start $start -End (Get-Date) }

    $rows += [PSCustomObject]@{
      PID      = $procId
      Script   = $script
      'Início'   = $(if ($start) { $start } else { '-' })
      'Duração'  = $(if ($dur)   { $dur }   else { '-' })
      Comando  = $cmd
    }
  }

if (-not $rows -or $rows.Count -eq 0) {
  Write-Host "Nenhum script Python (.py) em execução — ou sem permissões para ler CommandLine." -ForegroundColor Yellow
  Write-Host "Experimente executar o PowerShell como Administrador se os processos pertencem a outra conta/serviço."
} else {
  $rows | Sort-Object 'Início' | Format-Table -AutoSize | Out-String -Width 260 | Write-Host
}
