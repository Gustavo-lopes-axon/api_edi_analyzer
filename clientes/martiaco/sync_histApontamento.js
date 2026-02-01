/**
 * @param
 */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function formatarTempo(ms) {
    const totalSeconds = Math.floor(ms / 1000);
    const hours = String(Math.floor(totalSeconds / 3600)).padStart(2, '0');
    const minutes = String(Math.floor((totalSeconds % 3600) / 60)).padStart(2, '0');
    const seconds = String(totalSeconds % 60).padStart(2, '0');
    return `${hours}:${minutes}:${seconds}`;
}

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../.env') });
const { iniciarSincronizacao } = require('../../engines/mssqlClient.js');

const { logTaskStatus, initializeLogger } = require('../../engines/logger.js');
const { getMssqlConfig, getSupabaseConfig } = require('../../utils/config.js');

const clientKey = process.argv[2];

if (!clientKey) {
    console.error("ERRO: É obrigatório fornecer o nome do cliente.");
    console.log("Uso: node sync_histApontamento.js [nome_do_cliente]");
    process.exit(1);
}

const clientPrefix = clientKey.toUpperCase();
console.log(`--- INICIANDO PROCESSAMENTO PARA O CLIENTE: [${clientPrefix}] ---`);

initializeLogger(clientPrefix);

const BYTE_TO_MB = 1048576;
const mssqlConfig = getMssqlConfig(clientPrefix); 
const supabaseConfig = getSupabaseConfig(clientPrefix);

const queryHistApontamento = `
    SELECT 
        recno,
        DATAHORA as datahora,
        USUARIO as usuario,
        ODF as odf,
        REVISAO as revisao,
        NUMOPE as numope,
        MAQUINA as maquina,
        QTD as qtd,
        BOAS as boas,
        REFUGO as refugo,
        FALTANTE as faltante,
        RETRABALHO as retrabalho,
        CODIGO_PRODUTO as codigo_produto
    FROM VIEW_HISTORICO_APONTAMENTO (NOLOCK)
`;

const listaDeTarefas = [
    {
        nomeEmpresa: `${clientPrefix}-HistApontamento-Upsert`,
        mssqlConfig: mssqlConfig, 
        supabaseConfig: supabaseConfig,
        query: queryHistApontamento,
        rpcFunctionName: 'sincronizar_hist_apontamento',
        rpcParameterName: 'json_input',
        usaLotes: true,
    }
];

async function migracaoHistApontamento() {
    const inicioMigracao = Date.now();
    let tarefasComFalha = 0;

    console.log(`\n=== INICIANDO MÓDULO: HISTÓRICO DE APONTAMENTO(${listaDeTarefas.length} passos) ===`);

    for (let i = 0; i < listaDeTarefas.length; i++) {
        const tarefa = listaDeTarefas[i];
        console.log(`\n--- [TAREFA ${i + 1}/${listaDeTarefas.length}] --- INICIANDO: ${tarefa.nomeEmpresa} ---`);
        const inicioTarefa = Date.now();
        let status = 'SUCESSO';
        let mensagemErro = null;
        let registrosDaTarefa = 0;
        let bytesDaTarefa = 0;
        let resultadoTarefa = null;

        try {
            resultadoTarefa = await iniciarSincronizacao(tarefa);

            registrosDaTarefa = (resultadoTarefa && resultadoTarefa.totalRegistros) ? resultadoTarefa.totalRegistros : (typeof resultadoTarefa === 'number' ? resultadoTarefa : 0);
            bytesDaTarefa = (resultadoTarefa && resultadoTarefa.totalBytes) ? resultadoTarefa.totalBytes : 0;

            const fimTarefa = Date.now();
            const tempoTarefaMs = fimTarefa - inicioTarefa;
            const bytesTarefaMB = (bytesDaTarefa / BYTE_TO_MB).toFixed(2);

            console.log(`--- [TAREFA ${i + 1} CONCLUÍDA] (Tempo: ${formatarTempo(tempoTarefaMs)}) ---`);
            console.log(`📦 Tarefa ${i + 1}: registros enviados = ${registrosDaTarefa}${bytesDaTarefa ? ` (${bytesTarefaMB} MB)` : ''}`);

        } catch (error) {
            tarefasComFalha++;
            status = 'FALHA';
            mensagemErro = error.message;

            const fimTarefa = Date.now();
            const tempoTarefaMs = fimTarefa - inicioTarefa;

            console.error(`!!! ERRO NA TAREFA ${i + 1} (${tarefa.nomeEmpresa}) (Tempo: ${formatarTempo(tempoTarefaMs)}) !!!`);
            console.error(`Detalhe: ${mensagemErro}`);
            console.log(`--- [TAREFA ${i + 1} FALHOU, PARANDO O MÓDULO] ---`);

            break;

        } finally {
            const fimTarefa = Date.now();
            await logTaskStatus(
                tarefa, 
                status, 
                inicioTarefa, 
                fimTarefa, 
                mensagemErro, 
                resultadoTarefa,
                bytesDaTarefa
            );
        }
    }

    const fimMigracao = Date.now();
    const tempoTotalMs = fimMigracao - inicioMigracao;
    const tempoFormatado = formatarTempo(tempoTotalMs);

    console.log("=================================================");

    if (tarefasComFalha > 0) {
        console.log(`⚠️ === MÓDULO HISTÓRICO DE APONTAMENTO FALHOU! === ⚠️`);
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(1); 
    } else {
        console.log("🎉 === MÓDULO HISTÓRICO DE APONTAMENTO CONCLUÍDO COM SUCESSO! === 🎉");
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(0);
    }
}

migracaoHistApontamento();