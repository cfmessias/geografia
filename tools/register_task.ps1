# scripts\register_task.ps1
$TaskName = "Geografia_BuildAndPush"
$Repo     = "C:\PythonProjects\emStreamlit\Geografia"
$Runner   = Join-Path $Repo "scripts\run_build_and_push.ps1"

$Action  = New-ScheduledTaskAction -Execute "powershell.exe" `
           -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$Runner`""
$Trigger = New-ScheduledTaskTrigger -Daily -At 20:00

# Executa no contexto do utilizador atual (vai pedir credenciais se necessário)
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger `
  -Description "Run exec_build_country_seed.py then git add/commit/push" `
  -RunLevel Highest
