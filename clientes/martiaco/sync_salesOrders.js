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
    console.log("Uso: node sync_salesOrders.js [nome_do_cliente]");
    process.exit(1);
}

const clientPrefix = clientKey.toUpperCase();
console.log(`--- INICIANDO PROCESSAMENTO PARA O CLIENTE: [${clientPrefix}] ---`);

initializeLogger(clientPrefix);

const BYTE_TO_MB = 1048576;
const mssqlConfig = getMssqlConfig(clientPrefix); 
const supabaseConfig = getSupabaseConfig(clientPrefix);

const querySalesOrders = `
    SELECT
        P.numodf,
        P.numped AS order_number,
        REPLACE(P.CODPCA,'00000','') AS product_code,
        E.DESCRI AS product_name, 
        ISNULL(CAST(P.DATAHORA AS date), GETDATE()) AS order_date, 
        CAST(P.DTENPD AS date) AS delivery_date, 
        CAST(ISNULL(P.dtnego, P.DTENPD)AS date) AS negotiated_date,
        'pending' AS status, 
        'normal' AS priority,
        ISNULL(P.QTPEDI,0) AS qtd_total_order,
        ISNULL(P.QTDENT,0) AS qtd_total_delivery_order,
        CAST((ISNULL(P.QTPEDI,0) - ISNULL(P.QTDENT,0)) AS INT) AS quantity,
        ISNULL(CAST(ISNULL(P.VALUNI,0) AS decimal(19,6)),0) AS unit_price,
        ISNULL((CAST((ISNULL(P.QTPEDI,0) - ISNULL(P.QTDENT,0)) AS INT) * CAST(ISNULL(P.VALUNI,0) AS decimal(19,6))),0) AS total_price,
        ISNULL((CAST((ISNULL(P.QTPEDI,0) - ISNULL(P.QTDENT,0)) AS INT) * CAST(ISNULL(VALUNI,0) AS decimal(19,6))),0) AS total_value,
        ISNULL((CAST((ISNULL(P.QTPEDI,0) - ISNULL(QTDENT,0)) AS INT) * CAST(ISNULL(VALUNI,0) AS decimal(19,6))),0) AS total_amount,
        C.CGC AS cnpj, 
        P.ORDEMCOMPRA AS ordem_compra,
        COALESCE(T_ENG.ENG_STATUS, 'N') AS eng_concluido,
        P.R_E_C_N_O_ AS recno_id
    FROM PPEDLISE P (NOLOCK) 
    LEFT JOIN ESTOQUE E (NOLOCK) ON E.CODIGO = P.CODPCA 
    LEFT JOIN CLIENTES C (NOLOCK) ON C.CODIGO = P.CLIENTE
    LEFT JOIN (
        SELECT DISTINCT PRO.NUMPEC, 
                CASE WHEN PRO.CONCLUIDO = 'T' THEN 'S' ELSE 'N' END AS ENG_STATUS
        FROM PROCESSO PRO (NOLOCK)
        WHERE PRO.CONCLUIDO = 'T' 
    ) AS T_ENG ON T_ENG.NUMPEC = P.CODPCA
    WHERE P.SITUACAO in ('900')
    ORDER BY P.R_E_C_N_O_ ASC
`;

const querySalesOrders_Cleanup = `
    SELECT P.R_E_C_N_O_ AS recno_id
    FROM PPEDLISE P (NOLOCK) 
    WHERE SITUACAO in ('900')
`;

const listaDeTarefas = [
    {
        nomeEmpresa: `${clientPrefix}-SalesOrders-TRUNCATE`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: `SELECT 1 AS placeholder`,
        rpcFunctionName: 'truncate_sales_orders_tables',
        rpcParameterName: 'placeholder',
        usaLotes: false
    },
    {
        nomeEmpresa: `${clientPrefix}-SalesOrders-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: querySalesOrders,
        rpcFunctionName: 'sincronizar_sales_orders',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-SalesOrders-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: querySalesOrders_Cleanup,
        rpcFunctionName: 'cleanup_sales_orders',
        rpcParameterName: 'json_input_codes',
    },
];

async function migracaoSalesOrders() {
    const inicioMigracao = Date.now();
    let tarefasComFalha = 0;

    console.log(`\n=== INICIANDO MÓDULO: SALES ORDERS (${listaDeTarefas.length} passos) ===`);

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
        console.log(`⚠️ === MÓDULO SALES ORDERS FALHOU! === ⚠️`);
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(1); 
    } else {
        console.log("🎉 === MÓDULO SALES ORDERS CONCLUÍDO COM SUCESSO! === 🎉");
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(0);
    }
}

migracaoSalesOrders();