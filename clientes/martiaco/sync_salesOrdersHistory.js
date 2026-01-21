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
    console.log("Uso: node sync_salesOrdersHistory.js [nome_do_cliente]");
    process.exit(1);
}

const clientPrefix = clientKey.toUpperCase();
console.log(`--- INICIANDO PROCESSAMENTO PARA O CLIENTE: [${clientPrefix}] ---`);

initializeLogger(clientPrefix);

const BYTE_TO_MB = 1048576;
const mssqlConfig = getMssqlConfig(clientPrefix); 
const supabaseConfig = getSupabaseConfig(clientPrefix);

const querySalesOrdersHistory = `
    WITH UnifiedHistory AS (
        SELECT 
            RTRIM(LTRIM(U.numodf)) AS numodf, 
            U.numped AS order_number, 
            REPLACE(U.CODPCA,'00000','') AS product_code, 
            U.DATAHORA, U.DTENPD, U.dtnego, 
            U.ORDEMCOMPRA, U.CST_DATA_CRIACAO_PEDIDO, 
            ISNULL(U.VALUNI, 0) AS valuni_clean, 
            ISNULL(U.QTPEDI, U.QTDPED) AS qtd_total_order_raw,
            ISNULL(U.QTDENT, 0) AS qtd_total_delivery_order_raw,
            CONVERT(VARCHAR(20), U.CST_DATA_CRIACAO_PEDIDO, 112) + RTRIM(LTRIM(U.numodf)) + REPLACE(U.CODPCA,'00000','') AS recno_id,
            ROW_NUMBER() OVER (
                PARTITION BY U.numodf, REPLACE(U.CODPCA,'00000','')
                ORDER BY U.CST_DATA_CRIACAO_PEDIDO DESC, U.numodf DESC
            ) AS rn
        FROM (
            SELECT 
                numodf, numped, CODPCA, ORDEMCOMPRA, 
                TRY_CAST(DATAHORA AS DATE) AS DATAHORA,
                TRY_CAST(DTENPD AS DATE) AS DTENPD,
                TRY_CAST(dtnego AS DATE) AS dtnego,
                COALESCE(TRY_CAST(QTPEDI AS DECIMAL(19, 6)), 0) AS QTPEDI,  
                COALESCE(TRY_CAST(QTDENT AS DECIMAL(19, 6)), 0) AS QTDENT,  
                COALESCE(TRY_CAST(VALUNI AS DECIMAL(19, 6)), 0) AS VALUNI,  
                P.DATAHORA AS CST_DATA_CRIACAO_PEDIDO,  
                NULL AS QTDPED
            FROM PPEDLISE P (NOLOCK)
            WHERE P.SITUACAO <> '991' 
            UNION ALL 
            SELECT 
                NODF AS numodf, numped, CODPCA, NULL AS ORDEMCOMPRA,
                TRY_CAST(DATAHORA AS DATE) AS DATAHORA,
                TRY_CAST(DTENPD AS DATE) AS DTENPD,
                TRY_CAST(dtnego AS DATE) AS dtnego,
                NULL AS QTPEDI, 
                COALESCE(TRY_CAST(QTDENT AS DECIMAL(19, 6)), 0) AS QTDENT,  
                COALESCE(TRY_CAST(VALUNI AS DECIMAL(19, 6)), 0) AS VALUNI,  
                CST_DATA_CRIACAO_PEDIDO, 
                COALESCE(TRY_CAST(QTDPED AS DECIMAL(19, 6)), 0) AS QTDPED 
            FROM PEDRLISE R (NOLOCK)
            WHERE R.MOTCANC IS NULL AND CST_DATA_CRIACAO_PEDIDO IS NOT NULL
        ) AS U
    )
    SELECT
        recno_id,
        numodf,
        order_number,
        product_code,
        NULL AS product_name, 
        ISNULL(DATAHORA, GETDATE()) AS order_date,
        ISNULL(DTENPD, '1900-01-01') AS delivery_date,
        ISNULL(dtnego, ISNULL(DTENPD, '1900-01-01')) AS negotiated_date,
        'pending' AS status,
        'normal' AS priority,
        qtd_total_order_raw AS qtd_total_order,
        qtd_total_delivery_order_raw AS qtd_total_delivery_order,
        CAST((qtd_total_order_raw - qtd_total_delivery_order_raw) AS INT) AS quantity,
        REPLACE(CAST(valuni_clean AS VARCHAR(50)), ',', '.') AS unit_price,
        REPLACE(CAST((qtd_total_order_raw - qtd_total_delivery_order_raw) * valuni_clean AS VARCHAR(50)), ',', '.') AS total_price,
        REPLACE(CAST((qtd_total_order_raw - qtd_total_delivery_order_raw) * valuni_clean AS VARCHAR(50)), ',', '.') AS total_value,
        REPLACE(CAST(ISNULL((qtd_total_order_raw - qtd_total_delivery_order_raw) * valuni_clean, 0) AS VARCHAR(50)), ',', '.') AS total_amount,
        NULL AS cnpj,
        ORDEMCOMPRA AS ordem_compra,
        NULL AS eng_concluido,
        CST_DATA_CRIACAO_PEDIDO AS dt_criado_em
    FROM UnifiedHistory
    WHERE rn = 1
    ORDER BY numodf ASC
`;

const querySalesOrdersHistory_Cleanup = `
    SELECT numodf, order_number, product_code FROM
    (
        SELECT
            P.numodf, P.numped AS order_number, REPLACE(P.CODPCA,'00000','') AS product_code
        FROM PPEDLISE P (NOLOCK) 
        WHERE SITUACAO <>'991'
        UNION
        SELECT
            NODF AS numodf, numped AS order_number, REPLACE(CODPCA,'00000','') AS product_code
        FROM PEDRLISE R (NOLOCK)
        WHERE R.MOTCANC IS NULL AND CST_DATA_CRIACAO_PEDIDO IS NOT NULL
    ) AS TB
`;

const listaDeTarefas = [
    {
        nomeEmpresa: `${clientPrefix}-SalesOrdersHistory-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: querySalesOrdersHistory,
        rpcFunctionName: 'sincronizar_sales_orders_history',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-SalesOrdersHistory-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: querySalesOrdersHistory_Cleanup,
        rpcFunctionName: 'cleanup_sales_orders_history',
        rpcParameterName: 'json_input_codes',
    },
];

async function migracaoSalesOrdersHistory() {
    const inicioMigracao = Date.now();
    let tarefasComFalha = 0;

    console.log(`\n=== INICIANDO MÓDULO: SALES ORDERS HISTORY (${listaDeTarefas.length} passos) ===`);

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
        console.log(`⚠️ === MÓDULO SALES ORDERS HISTORY FALHOU! === ⚠️`);
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(1); 
    } else {
        console.log("🎉 === MÓDULO SALES ORDERS HISTORY CONCLUÍDO COM SUCESSO! === 🎉");
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(0);
    }
}

migracaoSalesOrdersHistory();