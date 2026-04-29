# ============================================================
# AGENDADOR DO PIPELINE CLIMA BRASIL
# Execute este script UMA VEZ como Administrador
# ============================================================

$NomeTarefa   = "PipelineClimaBrasil"
$Python       = "c:\Users\ASUS\pipeline-clima-brasil\venv\Scripts\python.exe"
$Script       = "c:\Users\ASUS\pipeline-clima-brasil\src\main.py"
$LogErro      = "c:\Users\ASUS\pipeline-clima-brasil\task_scheduler_erro.log"

# Remove tarefa anterior se existir
if (Get-ScheduledTask -TaskName $NomeTarefa -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $NomeTarefa -Confirm:$false
    Write-Host "Tarefa anterior removida." -ForegroundColor Yellow
}

# Acao: rodar python main.py
$Acao = New-ScheduledTaskAction `
    -Execute $Python `
    -Argument $Script `
    -WorkingDirectory "c:\Users\ASUS\pipeline-clima-brasil"

# Gatilho: todo dia a cada 1 hora, comecando agora
$Gatilho = New-ScheduledTaskTrigger `
    -RepetitionInterval (New-TimeSpan -Hours 1) `
    -Once `
    -At (Get-Date)

# Configuracoes: rodar mesmo sem usuario logado, nao parar se demorar
$Configuracao = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -StartWhenAvailable

# Registrar a tarefa
Register-ScheduledTask `
    -TaskName    $NomeTarefa `
    -Action      $Acao `
    -Trigger     $Gatilho `
    -Settings    $Configuracao `
    -RunLevel    Highest `
    -Force

Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host " Tarefa agendada com sucesso!" -ForegroundColor Green
Write-Host " Nome   : $NomeTarefa" -ForegroundColor Cyan
Write-Host " Roda   : a cada 1 hora" -ForegroundColor Cyan
Write-Host " Python : $Python" -ForegroundColor Cyan
Write-Host " Script : $Script" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Green
Write-Host ""
Write-Host "Para verificar: Get-ScheduledTask -TaskName '$NomeTarefa'" -ForegroundColor Gray
Write-Host "Para rodar agora: Start-ScheduledTask -TaskName '$NomeTarefa'" -ForegroundColor Gray
Write-Host "Para remover: Unregister-ScheduledTask -TaskName '$NomeTarefa'" -ForegroundColor Gray