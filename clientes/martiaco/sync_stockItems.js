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
const { getMssqlConfig, getSupabaseConfig } = require('../../engines/config.js');

const clientKey = process.argv[2];

if (!clientKey) {
    console.error("ERRO: É obrigatório fornecer o nome do cliente.");
    console.log("Uso: node sync_stockItems.js [nome_do_cliente]");
    process.exit(1);
}

const clientPrefix = clientKey.toUpperCase();
console.log(`--- INICIANDO PROCESSAMENTO PARA O CLIENTE: [${clientPrefix}] ---`);

initializeLogger(clientPrefix);

const BYTE_TO_MB = 1048576;
const mssqlConfig = getMssqlConfig(clientPrefix); 
const supabaseConfig = getSupabaseConfig(clientPrefix);

const queryStockItems = `
    WITH RankedStockItems AS (
        SELECT 
            COALESCE(NULLIF(LTRIM(RTRIM(CODIGO)), ''), 'NA') AS codigo,
            COALESCE(NULLIF(LTRIM(RTRIM(DESCRI)), ''), '(Sem Nome)') as nome, 
            '9b823b9b-04f0-4d66-bf3b-3d3c55938054' as category, 
            'UN' as unit, 
            REPLACE(CAST(ISNULL(SALDOREAL,0) AS VARCHAR(50)), ',', '.') as quantity,
            0 as min_stock, 
            '70fe3687-bc9d-4fe6-9a23-ce1514ac3660' as location,
            REPLACE(CAST(0 AS VARCHAR(50)), ',', '.') as cost, 
            REPLACE(CAST(ISNULL(VALPAGO,0) AS VARCHAR(50)), ',', '.') as price, 
            'active' as status,
            R_E_C_N_O_ AS recno_id,
            ROW_NUMBER() OVER (PARTITION BY LTRIM(RTRIM(CODIGO)) ORDER BY R_E_C_N_O_ DESC) as rn
        FROM ESTOQUE (NOLOCK)
        WHERE STATUS ='A' 
    )
    SELECT
        codigo, nome, category, unit, quantity, min_stock, location, cost, price, status, recno_id
    FROM RankedStockItems
    WHERE rn = 1
    ORDER BY codigo ASC
`;

const queryStockItems_Cleanup = `
    WITH RankedStockItems AS (
        SELECT 
            COALESCE(NULLIF(LTRIM(RTRIM(CODIGO)), ''), 'NA') AS codigo,
            R_E_C_N_O_ AS recno_id,
            ROW_NUMBER() OVER (PARTITION BY LTRIM(RTRIM(CODIGO)) ORDER BY R_E_C_N_O_ DESC) as rn
        FROM ESTOQUE (NOLOCK)
        WHERE STATUS ='A' 
    )
    SELECT
        codigo
    FROM RankedStockItems
    WHERE rn = 1
    ORDER BY codigo ASC
`;

const listaDeTarefas = [
    {
        nomeEmpresa: `${clientPrefix}-StockItems-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryStockItems,
        rpcFunctionName: 'sincronizar_stock_items',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-StockItems-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryStockItems_Cleanup,
        rpcFunctionName: 'cleanup_stock_items',
        rpcParameterName: 'json_input_codes',
    },
];

async function migracaoStockItems() {
    const inicioMigracao = Date.now();
    let tarefasComFalha = 0;

    console.log(`\n=== INICIANDO MÓDULO: STOCK ITEMS (${listaDeTarefas.length} passos) ===`);

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
        console.log(`⚠️ === MÓDULO STOCK ITEMS FALHOU! === ⚠️`);
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(1); 
    } else {
        console.log("🎉 === MÓDULO STOCK ITEMS CONCLUÍDO COM SUCESSO! === 🎉");
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(0);
    }
}

migracaoStockItems();