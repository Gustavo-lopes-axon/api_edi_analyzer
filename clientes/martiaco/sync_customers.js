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
    console.log("Uso: node sync_customers.js [nome_do_cliente]");
    process.exit(1);
}

const clientPrefix = clientKey.toUpperCase();
console.log(`--- INICIANDO PROCESSAMENTO PARA O CLIENTE: [${clientPrefix}] ---`);

initializeLogger(clientPrefix);

const BYTE_TO_MB = 1048576;
const mssqlConfig = getMssqlConfig(clientPrefix); 
const supabaseConfig = getSupabaseConfig(clientPrefix);

const queryCustomers = `
    SELECT
    CODIGO AS codigo, 
    MAX(rassoc) AS razao_social, 
    MAX(COALESCE(NULLIF(LTRIM(RTRIM(RESUMO)), ''), 'NA')) AS nome_fantasia, 
    MAX(COALESCE(NULLIF(LTRIM(RTRIM(CGC)), ''), 'NA')) AS cnpj, 
    MAX(INSC) AS inscricao_estadual, 
    MAX(endereco) AS logradouro,
    MAX(ibge_logradouro) AS numero, 
    MAX(COALESCE(NULLIF(LTRIM(RTRIM(complemento)), ''), '')) AS complemento,
    MAX(bairro) AS bairro,
    MAX(cidade) AS cidade,
    MAX(estado) AS estado, 
    MAX(cep) AS cep,
    MAX(COALESCE(NULLIF(LTRIM(RTRIM(TEL1)), ''), '')) AS telefone,
    MAX(COALESCE(NULLIF(LTRIM(RTRIM(email)), ''), '')) AS email, 
    MAX(COALESCE(NULLIF(LTRIM(RTRIM(CONTATO)), ''), '')) AS responsavel_comercial,
    (SELECT TOP 1 cl.DESCRI FROM SEGMENTO cl (nolock) WHERE cl.SEGMEN = c.segmento) AS segmento_industrial,
    (SELECT TOP 1 cl.DESCLASSE FROM CLAS_CLI cl (nolock) WHERE cl.CLASSE = c.classe) AS classe,
    (SELECT TOP 1 isnull(f.resumo,f.rassoc) FROM FORNECED f (nolock) 
    WHERE f.codigo = (SELECT TOP 1 CODIGO_FORNECEDOR FROM CONFIGURACAO_CLIENTE_EMPRESA (nolock) where CODIGO_CLIENTE = c.codigo) AND f.comissionado = 's') AS representante,
    'ativo' AS status
    FROM CLIENTES c (NOLOCK) 
    WHERE STATUS <> 'I' 
    GROUP BY CODIGO, c.segmento, c.classe 
    ORDER BY CODIGO ASC
`;

const queryCustomers_Cleanup = `
    SELECT 
        CODIGO AS codigo
    FROM CLIENTES c (NOLOCK) 
    WHERE STATUS <> 'I'
    GROUP BY CODIGO
    ORDER BY CODIGO ASC
`;

const listaDeTarefas = [
    {
        nomeEmpresa: `${clientPrefix}-Customers-Upsert`,
        mssqlConfig: mssqlConfig,
        supabaseConfig: supabaseConfig,
        query: queryCustomers,
        rpcFunctionName: 'sincronizar_customers',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-Customers-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryCustomers_Cleanup,
        rpcFunctionName: 'cleanup_customers',
        rpcParameterName: 'json_input_codes',
    },
];

async function migracaoCustomers() {
    const inicioMigracao = Date.now();
    let tarefasComFalha = 0;

    console.log(`\n=== INICIANDO MÓDULO: CUSTOMERS (${listaDeTarefas.length} passos) ===`);

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
        console.log(`⚠️ === MÓDULO CUSTOMERS FALHOU! === ⚠️`);
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(1); 
    } else {
        console.log("🎉 === MÓDULO CUSTOMERS CONCLUÍDO COM SUCESSO! === 🎉");
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(0);
    }
}

migracaoCustomers();