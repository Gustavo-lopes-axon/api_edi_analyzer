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
    console.log("Uso: node sync_nextFront.js [nome_do_cliente");
    process.exit(1);
}

const clientPrefix = clientKey.toUpperCase();
console.log(`--- INICIANDO SINCRONIZAÇÃO NEXT FRONT: [${clientPrefix}] ---`);

initializeLogger(clientPrefix);

const BYTE_TO_MB = 1048576;
const mssqlConfig = getMssqlConfig(clientPrefix);
const supabaseConfig = getSupabaseConfig(clientPrefix);

const queryNextFront = `
    SELECT 
        ID as id,
        R_E_C_N_O_ as recno,
        CODIGO_PECA as codigo_peca,
        NUMERO_ODF as numero_odf,
        DT_INICIO_OP as dt_inicio_op,
        DT_FIM_OP as dt_fim_op,
        HORA_INICIO as hora_inicio,
        HORA_FIM as hora_fim,
        DT_INICIO_IDEAL as dt_inicio_ideal,
        DT_FIM_IDEAL as dt_fim_ideal,
        HORA_INICIO_IDEAL as hora_inicio_ideal,
        HORA_FIM_IDEAL as hora_fim_ideal,
        CODIGO_MAQUINA as codigo_maquina,
        MACHINE_INDEX as machine_index,
        QTDE_ODF as qtde_odf,
        QTDE_APONTADA as qtde_apontada,
        QTD_REFUGO as qtd_refugo,
        DT_ENTREGA_ODF as dt_entrega_odf,
        NUMOPE as numope,
        NUMSEQ as numseq,
        FERRAMENTA as ferramenta,
        DIM1 as dim1,
        DIM2 as dim2,
        CONDIC as condic,
        CST_LISTA_CRITICA as cst_lista_critica,
        CST_PLANO_CORTE as cst_plano_corte,
        ATRASO as atraso,
        TEMPO_EXECUT as tempo_execut,
        TEMPO_REGULA as tempo_regula,
        CUTPLAN_MACHINE as cutplan_machine,
        AV_QTD as av_qtd,
        QTD_REFUGO_TOT as qtd_refugo_tot,
        APONTAMENTO_LIBERADO as apontamento_liberado,
        VALOR_VENDA as valor_venda,
        DEVELOP as develop,
        REWORK as rework,
        REVISAO as revisao,
        QTD_PROD as qtd_prod,
        TOOL as tool,
        QTD_FALTANTE as qtd_faltante,
        QTD_FALTANTE_TOT as qtd_faltante_tot,
        CUTTING_PLAN_RELEASED as cutting_plan_released,
        REMODEL_INDEX as remodel_index,
        GuidId as guidid,
        APONTAMENTO_LIBERADO_OLD as apontamento_liberado_old,
        CST_LISTA_CRITICA_OLD as cst_lista_critica_old,
        FASE as fase
    FROM TBL_AXON_NEXT_FRONT (NOLOCK)
`;

const listaDeTarefas = [
    {
        nomeEmpresa: `${clientPrefix}-NextFront-Upsert`,
        mssqlConfig: mssqlConfig, 
        supabaseConfig: supabaseConfig,
        query: queryNextFront,
        rpcFunctionName: 'sincronizar_next_front',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-NextFront-Clean`, 
        mssqlConfig: mssqlConfig,
        supabaseConfig: supabaseConfig,
        query: "SELECT 1 as id",
        rpcFunctionName: 'limpar_next_front_orfaos',
        rpcParameterName: 'json_input',
        usaLotes: false,
    }
];

async function migracaoNextFront() {
    const inicioMigracao = Date.now();
    let tarefasComFalha = 0;

    console.log(`\n=== INICIANDO MÓDULO: NEXT FRONT ===`);

    for (let i = 0; i < listaDeTarefas.length; i++) {
        const tarefa = listaDeTarefas[i];
        console.log(`\n--- INICIANDO: ${tarefa.nomeEmpresa} ---`);
        const inicioTarefa = Date.now();
        let status = 'SUCESSO';
        let mensagemErro = null;
        let resultadoTarefa = null;

        try {
            resultadoTarefa = await iniciarSincronizacao(tarefa);

            const registros = (resultadoTarefa && resultadoTarefa.totalRegistros) ? resultadoTarefa.totalRegistros : 0;
            const bytes = (resultadoTarefa && resultadoTarefa.totalBytes) ? resultadoTarefa.totalBytes : 0;
            const tempoTarefaMs = Date.now() - inicioTarefa;

            console.log(`--- [CONCLUÍDA] (Tempo: ${formatarTempo(tempoTarefaMs)}) ---`);
            console.log(`📦 Registros processados: ${registros} (${(bytes / BYTE_TO_MB).toFixed(2)} MB)`);

        } catch (error) {
            tarefasComFalha++;
            status = 'FALHA';
            mensagemErro = error.message;
            console.error(`!!! ERRO NA TAREFA: ${error.message} !!!`);
            break;
        } finally {
            await logTaskStatus(tarefa, status, inicioTarefa, Date.now(), mensagemErro, resultadoTarefa);
        }
    }

    console.log("=================================================");
    console.log(tarefasComFalha > 0 ? "⚠️ FALHA NO PROCESSO" : "🎉 SUCESSO NO PROCESSO");
    console.log(`⏱️ Tempo Total: ${formatarTempo(Date.now() - inicioMigracao)}`);
    process.exit(tarefasComFalha > 0 ? 1 : 0);
}

migracaoNextFront();