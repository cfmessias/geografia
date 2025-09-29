# tools\run_build_and_push.ps1
#requires -version 5.1
$ErrorActionPreference = 'Stop'

# === CONFIG ===
$Repo     = "C:\PythonProjects\emStreamlit\Geografia"
$PyScript = Join-Path $Repo "scripts\exec_build_country_seed.py"
$VenvPy   = Join-Path $Repo ".venv\Scripts\python.exe"
$LogDir   = Join-Path $Repo "logs"
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }
$LogFile  = Join-Path $LogDir ("run_build_and_push_{0}.log" -f (Get-Date -Format "yyyyMMdd_HHmmss"))

if (-not (Test-Path $PyScript)) { throw "Script Python não encontrado: $PyScript" }

# === Funções utilitárias ===
function Test-Cmd($cmd, $args=@("--version")) {
    try {
        $p = Start-Process -FilePath $cmd -ArgumentList $args -NoNewWindow -PassThru -Wait -ErrorAction Stop
        return ($p.ExitCode -eq 0)
    } catch { return $false }
}

function Pick-Python {
    $candidates = @()
    if (Test-Path $VenvPy) { $candidates += ,@($VenvPy) }
    $candidates += ,@("python")
    $candidates += ,@("python3")
    $candidates += ,@("py","-3")
    $candidates += ,@("py")

    foreach ($c in $candidates) {
        $cmd  = $c[0]
        $args = @()
        if ($c.Count -gt 1) { $args = $c[1..($c.Count-1)] }
        if (Test-Cmd $cmd $args) { return @{ cmd=$cmd; args=$args } }
    }
    throw "Não encontrei um executável Python utilizável (.venv, python, python3, py)."
}

$py = Pick-Python
$PythonCmd  = $py.cmd
$PythonArgs = $py.args

Push-Location $Repo
try {
    Write-Host ("Running {0} with: {1} {2}" -f $PyScript, $PythonCmd, ($PythonArgs -join " "))

    # Executa o Python (args em array!)
    & $PythonCmd @PythonArgs "-u" $PyScript 2>&1 | Tee-Object -FilePath $LogFile -Append
    if ($LASTEXITCODE -ne 0) { throw "Python exited with code $LASTEXITCODE" }

    # Git add/commit/push apenas se houver mudanças
    $status = git status --porcelain
    if ([string]::IsNullOrWhiteSpace($status)) {
        "Sem alterações para commit." | Tee-Object -FilePath $LogFile -Append
    } else {
        git add . | Tee-Object -FilePath $LogFile -Append
        $msg = "build: country seed ({0})" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
        git commit -m $msg | Tee-Object -FilePath $LogFile -Append

        $branch = (git rev-parse --abbrev-ref HEAD).Trim()
        if ([string]::IsNullOrWhiteSpace($branch)) { $branch = "main" }
        try { git push origin $branch | Tee-Object -FilePath $LogFile -Append }
        catch { git push -u origin $branch | Tee-Object -FilePath $LogFile -Append }
        "Push efetuado para '$branch'." | Tee-Object -FilePath $LogFile -Append
    }

    "Done. Log: $LogFile" | Tee-Object -FilePath $LogFile -Append
}
finally {
    Pop-Location
}
