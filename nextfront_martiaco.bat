@echo off
cd /d "C:\Caminho\Para\Seu\Projeto\etl-engine"

:loop
echo [%date% %time%] Iniciando Sincronizacao NextFront...

node clientes/martiaco/sync_nextFront.js MARTIACO

echo [%date% %time%] Aguardando 60 segundos para a proxima rodada...
echo ----------------------------------------------------------

timeout /t 60 /nobreak

goto loop