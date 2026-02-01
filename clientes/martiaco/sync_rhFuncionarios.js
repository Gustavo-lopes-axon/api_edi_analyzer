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
    console.log("Uso: node sync_rhFuncionarios.js [nome_do_cliente]");
    process.exit(1);
}

const clientPrefix = clientKey.toUpperCase();
console.log(`--- INICIANDO PROCESSAMENTO PARA O CLIENTE: [${clientPrefix}] ---`);

initializeLogger(clientPrefix);

const BYTE_TO_MB = 1048576;
const mssqlConfig = getMssqlConfig(clientPrefix); 
const supabaseConfig = getSupabaseConfig(clientPrefix);

const queryFuncionarios = `
    SELECT 
        LTRIM(RTRIM(MATRIC)) AS matricula,
        LTRIM(RTRIM(CRACHA)) AS numero_cracha,
        LTRIM(RTRIM(FUNCIONARIO)) AS nome_completo,
        LTRIM(RTRIM(CPF)) AS cpf,
        LTRIM(RTRIM(EMAIL)) AS email,
        LTRIM(RTRIM(FONE)) AS telefone,
        DT_ADM AS data_admissao,
        DT_DEM AS data_demissao,
        CASE 
            WHEN DT_DEM IS NULL THEN 'ativo' 
            ELSE 'desligado' 
        END AS status,
        LTRIM(RTRIM(OBS)) AS observacoes,
        R_E_C_N_O_ AS recno_id
    FROM dbo.FUNCIONARIOS (NOLOCK)
    ORDER BY MATRIC ASC
`;

const listaDeTarefas = [
    {
        nomeEmpresa: `${clientPrefix}-RH-Funcionarios-Upsert`,
        mssqlConfig: mssqlConfig, 
        supabaseConfig: supabaseConfig,
        query: queryFuncionarios,
        rpcFunctionName: 'sincronizar_rh_funcionarios',
        rpcParameterName: 'json_input',
        usaLotes: true,
    }
];

async function migracaoFuncionarios() {
    const inicioMigracao = Date.now();
    let tarefasComFalha = 0;

    console.log(`\n=== INICIANDO MÓDULO: RH FUNCIONARIOS (${listaDeTarefas.length} passos) ===`);

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
        console.log(`⚠️ === MÓDULO RH FUNCIONARIOS FINALIZADO COM ERROS! === ⚠️`);
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        setTimeout(() => process.exit(0), 1000); 
    } else {
        console.log("🎉 === MÓDULO RH FUNCIONARIOS CONCLUÍDO COM SUCESSO! === 🎉");
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        setTimeout(() => process.exit(0), 1000);
    }
}

migracaoFuncionarios().catch(err => {
    console.error("ERRO NÃO TRATADO:", err);
    process.exit(0); 
});