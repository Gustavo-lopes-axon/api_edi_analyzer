const path = require('path');

require('dotenv').config({ path: path.join(__dirname, '../.env') }); 

const { spawn } = require('child_process');

function formatarTempo(ms) {
    const totalSeconds = Math.floor(ms / 1000);
    const hours = String(Math.floor(totalSeconds / 3600)).padStart(2, '0');
    const minutes = String(Math.floor((totalSeconds % 3600) / 60)).padStart(2, '0');
    const seconds = String(totalSeconds % 60).padStart(2, '0');
    return `${hours}:${minutes}:${seconds}`;
}

function runModule(moduleName, client, workingDir) {
    return new Promise((resolve) => {
        const command = 'node';
        const scriptPath = path.join(workingDir, moduleName);
        const args = [scriptPath, client];

        const child = spawn(command, args, {
            stdio: 'inherit',
            cwd: workingDir
        });

        child.on('close', (code) => {
            const status = code === 0 ? 'SUCESSO' : 'FALHA';
            console.log(`\n[${status}] Módulo ${moduleName} finalizado com código ${code}.`);
            resolve({ moduleName, code });
        });
    });
}

async function dispararTodasAsTarefas(clientKey, modules, workingDir) {
    const startTime = Date.now();
    const clientPrefix = clientKey.toUpperCase();

    console.log(`\n=================================================`);
    console.log(`=== INICIANDO DISPARADOR PARA CLIENTE: [${clientPrefix}] ===`);
    console.log(`=================================================\n`);

    const tasks = modules.map(moduleName => runModule(moduleName, clientKey, workingDir));
    const results = await Promise.all(tasks);

    const failedResults = results.filter(r => r.code !== 0);
    const duration = Date.now() - startTime;

    console.log('\n=================================================');
    console.log(`FIM DA EXECUÇÃO | Tempo Total: ${formatarTempo(duration)}`);
    console.log(`Total de Módulos: ${modules.length} | Falhas: ${failedResults.length}`);
    console.log('=================================================\n');

    process.exit(failedResults.length > 0 ? 1 : 0);
}

module.exports = { dispararTodasAsTarefas };