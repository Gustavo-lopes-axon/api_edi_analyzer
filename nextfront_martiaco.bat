@echo off
cd /d "C:\integracoes\motor-integracao"

:loop
cls
echo ==========================================================
echo    MOTOR DE INTEGRACAO AXON - SINCRONIZACAO REAL-TIME
echo ==========================================================
echo [%date% %time%] Iniciando Sincronizacao NextFront...

node clientes/martiaco/sync_nextFront.js MARTIACO

echo.
echo [%date% %time%] Aguardando 60 segundos para a proxima rodada...
echo ----------------------------------------------------------

timeout /t 60 /nobreak

goto loop