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
    console.log("Uso: node sync_purchaseOrders.js [nome_do_cliente]");
    process.exit(1);
}

const clientPrefix = clientKey.toUpperCase();
console.log(`--- INICIANDO PROCESSAMENTO PARA O CLIENTE: [${clientPrefix}] ---`);

initializeLogger(clientPrefix);

const BYTE_TO_MB = 1048576;
const mssqlConfig = getMssqlConfig(clientPrefix); 
const supabaseConfig = getSupabaseConfig(clientPrefix);

const queryPurchaseOrders = `
    select 
    FORNECE AS fornecedor,
    ORIGEM as origem,
    NDOC as pedido,
    ITEM product_code,
    DESCRI as descricao,
    cast(DATAHORA as date) as criado_em,
    isnull(QTPED,0) as qtde_pedido,
    isnull(QTENT,0) as qtde_entregue,
    (ISNULL(QTPED,0) - ISNULL(QTENT,0)) as saldo,
    isnull(VALOR,0) as valor_unitario,
    ((ISNULL(QTPED,0) - ISNULL(QTENT,0)) * isnull(VALOR,0) ) as valor_total,
    cast(DTPED as date) as data_entrega,
    cast(isnull(DTNEGO,DTPED) as date) as data_negociada,
    ISNULL(NULLIF(LTRIM(RTRIM(UNIDADE)), ''), 'UN') AS unidade,
    (SELECT TOP 1 G.DESC_GRU FROM GRUPOCPR G (NOLOCK) WHERE G.GRUPO = PARCLISE.GRUPO) as centro_custo,
    ISNULL((
    SELECT top 1 (SELECT PC.DESCRICAO FROM PAG_CONDICAO PC (NOLOCK) WHERE PP.REGRA_PARC = PC.CODIGO) AS REGRA_PARC 
    FROM PEDIDO_FOR PP (NOLOCK) where PP.NDOC = PARCLISE.NDOC
    ),'Não Informado') as cond_pgto,
    ISNULL((
    SELECT top 1 SUM(VALOR) FROM PEDIDOFOR_SINAL_PARC PP (NOLOCK) where PP.NDOC = PARCLISE.NDOC
    ),0) as valor_adiantamento_compra,
    ISNULL((
    SELECT top 1 (SELECT TOP 1 CASE CP.STATUS WHEN 'P' then 'Pago' else 'Aberto' end FROM CONTASP CP (NOLOCK) where CP.R_E_C_N_O_ = PP.TITULO) FROM PEDIDOFOR_SINAL_PARC PP (NOLOCK) where PP.NDOC = PARCLISE.NDOC
    ),'') as status_adiantamento_compra,
    (SELECT top 1 replace(replace(replace(CGC,'.',''),'/',''),'-','') FROM FORNECED f (NOLOCK) WHERE F.CODIGO = PARCLISE.FORNECE) as fornecedor_cnpj,
    R_E_C_N_O_ AS recno_id,
    'em_aberto' as status
    from PARCLISE (NOLOCK)
    ORDER BY NDOC ASC
`;

const queryPurchaseOrders_Cleanup = `
    SELECT 
        R_E_C_N_O_ AS recno_id
    FROM PARCLISE (NOLOCK)
    ORDER BY R_E_C_N_O_ ASC
`;

const listaDeTarefas = [
    {
        nomeEmpresa: `${clientPrefix}-PurchaseOrders-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryPurchaseOrders,
        rpcFunctionName: 'sincronizar_purchase_orders',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-PurchaseOrders-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryPurchaseOrders_Cleanup, 
        rpcFunctionName: 'cleanup_purchase_orders', 
        rpcParameterName: 'json_input_codes',
    },
];

async function migracaoPurchaseOrders() {
    const inicioMigracao = Date.now();
    let tarefasComFalha = 0;

    console.log(`\n=== INICIANDO MÓDULO: PURCHASE ORDERS (${listaDeTarefas.length} passos) ===`);

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
        console.log(`⚠️ === MÓDULO PURCHASE ORDERS FALHOU! === ⚠️`);
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(1); 
    } else {
        console.log("🎉 === MÓDULO PURCHASE ORDERS CONCLUÍDO COM SUCESSO! === 🎉");
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(0);
    }
}

migracaoPurchaseOrders();