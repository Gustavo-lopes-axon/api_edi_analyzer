const { createClient } = require('@supabase/supabase-js');
const { v4: uuidv4 } = require('uuid');

let supabaseClient = null;
let CURRENT_EXECUTION_ID = null;

function initializeLogger(clientPrefix) {
    const prefix = clientPrefix.toUpperCase();
    const supabaseUrl = process.env[`${prefix}_SUPABASE_URL`];
    const supabaseServiceRoleKey = process.env[`${prefix}_SUPABASE_KEY`];

    if (!supabaseUrl || !supabaseServiceRoleKey) {
        throw new Error(`ERRO: As variáveis ${prefix}_SUPABASE_URL ou ${prefix}_SUPABASE_KEY não foram encontradas no ambiente.`);
    }

    supabaseClient = createClient(supabaseUrl, supabaseServiceRoleKey);
    CURRENT_EXECUTION_ID = uuidv4();
    console.log(`[LOGGER] Cliente Supabase (${prefix}) configurado com sucesso. ID: ${CURRENT_EXECUTION_ID}`);
}

/**
 * @param {object} tarefa
 * @param {string} status
 * @param {number} inicioTarefa
 * @param {number} fimTarefa
 * @param {string|null} mensagemErro
 * @param {number|object} totalRecordsSent
 * @param {number} dataSizeBytes
 * @param {number} totalRecordsSource
 */
async function logTaskStatus(tarefa, status, inicioTarefa, fimTarefa, mensagemErro, totalRecordsSent = 0, dataSizeBytes = 0, totalRecordsSource = 0) {
    if (!supabaseClient || !CURRENT_EXECUTION_ID) {
        console.error("ERRO CRÍTICO: Logger não foi inicializado.");
        return;
    }

    if (typeof totalRecordsSent === 'object' && totalRecordsSent !== null) {
        const resultadoMotor = totalRecordsSent;
        totalRecordsSource = resultadoMotor.totalOrigem || 0;
        dataSizeBytes = resultadoMotor.totalBytes || 0;
        totalRecordsSent = resultadoMotor.totalRegistros || 0;
    }

    const duration_ms = fimTarefa ? (fimTarefa - inicioTarefa) : null;
    const logTime = fimTarefa ? new Date(fimTarefa).toISOString() : new Date().toISOString();

    const [clientPrefix, ...moduleParts] = tarefa.nomeEmpresa.split('-');
    const moduleName = moduleParts.join('-');

    const logData = {
        execution_id: CURRENT_EXECUTION_ID,
        task_name: tarefa.nomeEmpresa,
        client_name: clientPrefix,
        module_name: moduleName,
        status: status,
        start_time: new Date(inicioTarefa).toISOString(),
        end_time: logTime,
        last_execution_time: logTime,
        duration_ms: duration_ms,
        total_records_sent: totalRecordsSent,
        data_size_bytes: dataSizeBytes,
        error_message: mensagemErro,
        details: {
            total_records_sent: totalRecordsSent,
            data_size_bytes: dataSizeBytes,
            total_records_source: totalRecordsSource
        }
    };

    const { error } = await supabaseClient
        .from('sincronizacao_logs')
        .upsert(logData, {
            onConflict: 'execution_id, task_name',
            ignoreDuplicates: false
        });

    if (error) {
        console.error(`Falha CRÍTICA ao registrar log: ${error.message}`);
    } else {
        console.log(`[LOGGER] Log para ${tarefa.nomeEmpresa} registrado com sucesso. (${totalRecordsSent}/${totalRecordsSource})`);
    }
}

module.exports = { logTaskStatus, initializeLogger, CURRENT_EXECUTION_ID };