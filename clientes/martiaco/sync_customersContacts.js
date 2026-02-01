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
    console.log("Uso: node sync_customersContacts.js [nome_do_cliente]");
    process.exit(1);
}

const clientPrefix = clientKey.toUpperCase();
console.log(`--- INICIANDO PROCESSAMENTO PARA O CLIENTE: [${clientPrefix}] ---`);

initializeLogger(clientPrefix);

const BYTE_TO_MB = 1048576;
const mssqlConfig = getMssqlConfig(clientPrefix); 
const supabaseConfig = getSupabaseConfig(clientPrefix);

const queryCustomerContacts = `
    SELECT
    CC.R_E_C_N_O_ AS recno_id,
    codigo as codigo_cliente,
    ISNULL(LTRIM(RTRIM(nome)), '') AS nome, 
    LTRIM(RTRIM(funcao)) AS funcao,
    fone,
    email,
    cep,
    municipio,
    uf,
    logradouro,
    numero,
    bairro,
    complemento,
    pais,
    (SELECT TOP 1 C.NOME FROM CARGOS c (nolock) WHERE C.R_E_C_N_O_ = CC.RECNO_CARGO) AS cargo
    FROM CONTATOS_CLIENTE CC (nolock)
    ORDER BY CC.R_E_C_N_O_ ASC
`;

const queryCustomerContacts_Cleanup = `
    SELECT 
        CC.R_E_C_N_O_ AS recno_id
    FROM CONTATOS_CLIENTE CC (nolock)
    ORDER BY CC.R_E_C_N_O_ ASC
`;

const listaDeTarefas = [
    {
        nomeEmpresa: `${clientPrefix}-CustomerContacts-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryCustomerContacts,
        rpcFunctionName: 'sincronizar_customer_contacts',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-CustomerContacts-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryCustomerContacts_Cleanup,
        rpcFunctionName: 'cleanup_customer_contacts',
        rpcParameterName: 'json_input_codes',
    },
];

async function migracaoCustomerContacts() {
    const inicioMigracao = Date.now();
    let tarefasComFalha = 0;

    console.log(`\n=== INICIANDO MÓDULO: CUSTOMERS CONTACTS (${listaDeTarefas.length} passos) ===`);

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

    if (tarefasComFalha > 0) {
        console.log(`⚠️ === MÓDULO CUSTOMERS CONTACTS FALHOU! === ⚠️`); 
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(1); 
    } else {
        console.log("🎉 === MÓDULO CUSTOMERS CONTACTS CONCLUÍDO COM SUCESSO! === 🎉"); 
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(0);
    }
}

migracaoCustomerContacts();