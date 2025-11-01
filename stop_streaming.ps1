# Script pour arrêter tous les processus de streaming

Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host "="*69 -ForegroundColor Cyan
Write-Host "🛑 ARRÊT DES PROCESSUS DE STREAMING" -ForegroundColor Red
Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host "="*69 -ForegroundColor Cyan
Write-Host ""

# Trouver et arrêter les processus Python liés au projet
$processes = Get-Process python -ErrorAction SilentlyContinue | Where-Object {
    $_.CommandLine -like "*simulateur*" -or 
    $_.CommandLine -like "*test_streaming*" -or 
    $_.CommandLine -like "*resource_manager*" -or
    $_.CommandLine -like "*maintenance_predictor*"
}

if ($processes) {
    Write-Host "📋 Processus trouvés:" -ForegroundColor Yellow
    foreach ($proc in $processes) {
        Write-Host "   • PID $($proc.Id) - $($proc.ProcessName)" -ForegroundColor Gray
    }
    Write-Host ""
    
    Write-Host "🔄 Arrêt en cours..." -ForegroundColor Yellow
    $processes | Stop-Process -Force
    Start-Sleep -Seconds 1
    
    Write-Host "✅ Tous les processus ont été arrêtés!" -ForegroundColor Green
} else {
    Write-Host "ℹ️  Aucun processus de streaming en cours" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host "="*69 -ForegroundColor Cyan
