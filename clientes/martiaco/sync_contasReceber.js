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
    console.log("Uso: node sync_contasReceber.js [nome_do_cliente]");
    process.exit(1);
}

const clientPrefix = clientKey.toUpperCase();
console.log(`--- INICIANDO PROCESSAMENTO PARA O CLIENTE: [${clientPrefix}] ---`);

initializeLogger(clientPrefix);

const BYTE_TO_MB = 1048576;
const mssqlConfig = getMssqlConfig(clientPrefix); 
const supabaseConfig = getSupabaseConfig(clientPrefix);

const queryContasReceber = `
    SELECT
    CODIGO AS codigo,
    COALESCE(NULLIF(LTRIM(RTRIM(DOCUMENTO)), ''), '(Sem Documento)') AS documento,
    ISNULL(cast(DATAEMI as date), GETDATE()) AS data_emissao,
    cast(VENCIMENTO as date) AS data_vencimento,
    cast(DATAPAGO as date) AS data_pagamento,
    REPLACE(CAST(ISNULL(valor,0) AS VARCHAR(50)), ',', '.') as valor_bruto,
    REPLACE(CAST(ISNULL(DESCONTO,0) AS VARCHAR(50)), ',', '.') as descontos,
    REPLACE(CAST(ISNULL(juros,0) AS VARCHAR(50)), ',', '.') AS juros,
    REPLACE(CAST(ISNULL(VALORLIQUIDO,0) AS VARCHAR(50)), ',', '.') AS valor_liquido,
    CASE 
        WHEN status = 'R' THEN 'recebido'
        WHEN status = 'A' AND CAST(VENCIMENTO AS date) < CAST(GETDATE() AS date) AND DATAPAGO IS NULL THEN 'atrasado'
        WHEN status = 'A' THEN 'em_aberto'
    END AS status,
    (SELECT TOP 1 G.DESC_GRU FROM GRUPOCPR G (NOLOCK) WHERE G.GRUPO = C.GRUPO) AS centro_custo,
    C.R_E_C_N_O_ AS recno_id
    FROM CONTASR C (NOLOCK)
    WHERE 
        STATUS <> 'I'
        AND C.status IN ('A', 'R')
        AND YEAR(C.DATAEMI) >= 2025
    ORDER BY data_vencimento ASC
`;

const queryContasReceber_Cleanup = `
    SELECT 
        C.R_E_C_N_O_ AS recno_id
    FROM CONTASR C (NOLOCK)
    WHERE 
        STATUS <> 'I'
        AND C.status IN ('A', 'R')
        AND YEAR(C.DATAEMI) >= 2025
    ORDER BY C.R_E_C_N_O_ ASC
`;

const listaDeTarefas = [
    {
        nomeEmpresa: `${clientPrefix}-ContasReceber-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryContasReceber,
        rpcFunctionName: 'sincronizar_contas_receber',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-ContasReceber-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryContasReceber_Cleanup, 
        rpcFunctionName: 'cleanup_contas_receber', 
        rpcParameterName: 'json_input_codes',
    },
];

async function migracaoContasReceber() {
    const inicioMigracao = Date.now();
    let tarefasComFalha = 0;

    console.log(`\n=== INICIANDO MÓDULO: CONTAS RECEBER (${listaDeTarefas.length} passos) ===`);

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
        console.log(`⚠️ === MÓDULO CONTAS RECEBER FALHOU! === ⚠️`);
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(1); 
    } else {
        console.log("🎉 === MÓDULO CONTAS RECEBER CONCLUÍDO COM SUCESSO! === 🎉");
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(0);
    }
}

migracaoContasReceber();