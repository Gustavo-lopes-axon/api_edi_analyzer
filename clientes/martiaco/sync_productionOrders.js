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
    console.log("Uso: node sync_productionOrders.js [nome_do_cliente]");
    process.exit(1);
}

const clientPrefix = clientKey.toUpperCase();
console.log(`--- INICIANDO PROCESSAMENTO PARA O CLIENTE: [${clientPrefix}] ---`);

initializeLogger(clientPrefix);

const BYTE_TO_MB = 1048576;
const mssqlConfig = getMssqlConfig(clientPrefix); 
const supabaseConfig = getSupabaseConfig(clientPrefix);

const queryProductionOrders = `
    WITH RankedOrders AS (
        SELECT 
            NUMODF as order_number,
            CODPCA as code,
            E.DESCRI AS product_name,
            CAST((ISNULL(QTPEDI,0) - ISNULL(QTDENT,0)) AS INT) AS quantity,
            'pending' AS status,
            'Linha A' AS production_line,
            CAST(dtinicio AS date) as start_date,
            CAST(ISNULL(dtnego, DTENPD) AS date) as delivery_date,
            0 as estimated_hours,
            'Materiais para ' + E.DESCRI as materials,
            'Produzir ' + CAST(CAST((ISNULL(QTPEDI,0) - ISNULL(QTDENT,0)) AS INT) AS VARCHAR(50)) + ' unidades de ' + E.DESCRI AS instructions,
            0 AS timer_elapsed_secounds,
            0 AS timer_paused_seconds,
            'FALSE' as timer_is_running,
            ISNULL((
                SELECT TOP 1 PEDIDO FROM (
                    SELECT PCP.ODFPED AS ODF_PEDIDO, PCP.NUMODF, DTENPD, 
                    (SELECT P1.NUMPED FROM PPEDLISE P1 (NOLOCK) WHERE P1.NUMODF = PCP.ODFPED) AS PEDIDO
                    FROM PPEDLISE PP (NOLOCK)
                    INNER JOIN PCP_ODF_PEDIDO PCP (NOLOCK) ON PP.NUMODF = PCP.ODFPED
                    WHERE PCP.NUMODF IN (SELECT ISNULL(PP1.NUMODF,0) FROM PPEDLISE PP1 (NOLOCK))
                    AND PCP.NUMODF = P.NUMODF
                ) AS TB
            ),'991') AS sales_order,
            P.R_E_C_N_O_ AS recno_id,
            ROW_NUMBER() OVER (
                PARTITION BY NUMODF  
                ORDER BY P.R_E_C_N_O_ DESC 
            ) AS rn
        FROM PPEDLISE P (NOLOCK)
        INNER JOIN ESTOQUE E (NOLOCK) ON E.CODIGO = P.CODPCA 
        WHERE SITUACAO ='991'
    )
    SELECT 
        order_number, code, product_name, quantity, status, production_line, start_date, delivery_date, estimated_hours, materials, instructions, timer_elapsed_secounds, timer_paused_seconds, timer_is_running, sales_order, recno_id
    FROM RankedOrders
    WHERE rn = 1 
    ORDER BY order_number ASC;
`;

const queryProductionOrders_Cleanup = `
    SELECT 
        P.R_E_C_N_O_ AS recno_id
    FROM PPEDLISE P (NOLOCK)
    INNER JOIN ESTOQUE E (NOLOCK) ON E.CODIGO = P.CODPCA 
    WHERE SITUACAO ='991'
    ORDER BY P.R_E_C_N_O_ ASC
`;

const listaDeTarefas = [
    {
        nomeEmpresa: `${clientPrefix}-ProductionOrders-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryProductionOrders,
        rpcFunctionName: 'sincronizar_production_orders',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-ProductionOrders-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryProductionOrders_Cleanup, 
        rpcFunctionName: 'cleanup_production_orders', 
        rpcParameterName: 'json_input_codes',
    },
];

async function migracaoProductionOrders() {
    const inicioMigracao = Date.now();
    let tarefasComFalha = 0;

    console.log(`\n=== INICIANDO MÓDULO: PRODUCTION ORDERS (${listaDeTarefas.length} passos) ===`);

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
        console.log(`⚠️ === MÓDULO PRODUCTION ORDERS FALHOU! === ⚠️`);
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(1); 
    } else {
        console.log("🎉 === MÓDULO PRODUCTION ORDERS CONCLUÍDO COM SUCESSO! === 🎉");
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(0);
    }
}

migracaoProductionOrders();