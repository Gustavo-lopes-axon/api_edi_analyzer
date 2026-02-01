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
    console.log("Uso: node sync_bomItems.js [nome_do_cliente]");
    process.exit(1);
}

const clientPrefix = clientKey.toUpperCase();
console.log(`--- INICIANDO PROCESSAMENTO PARA O CLIENTE: [${clientPrefix}] ---`);

initializeLogger(clientPrefix);

const BYTE_TO_MB = 1048576;
const mssqlConfig = getMssqlConfig(clientPrefix); 
const supabaseConfig = getSupabaseConfig(clientPrefix);

const queryBomItems = `
    WITH RankedBomItems AS (
        SELECT
            LTRIM(RTRIM(p.NUMPEC)) AS code, 
            ISNULL(LTRIM(RTRIM(o.NUMITE)),'') AS codigo,
            COALESCE(NULLIF(LTRIM(RTRIM(o.DESCRI)), ''), '(Componente sem nome)') AS componente,
            CASE CONDIC 
                WHEN 'P' THEN 'Componente' 
                WHEN 'M' THEN 'Matéria-prima' 
                WHEN 'S' THEN 'Terceirizado' END AS tipo,
            ISNULL(o.EXECUT,0) AS quantidade,
            CASE CONDIC 
                WHEN 'P' THEN 'Peça' 
                WHEN 'M' THEN 'Kg' 
                WHEN 'S' THEN 'Peça' END AS unidade,
            0 AS custo_unitario,
            0 AS custo_total,
            o.R_E_C_N_O_ AS recno_id,
            ROW_NUMBER() OVER (
                PARTITION BY LTRIM(RTRIM(p.NUMPEC)), ISNULL(LTRIM(RTRIM(o.NUMITE)),'')
                ORDER BY o.R_E_C_N_O_ DESC
            ) AS rn
        FROM dbo.PROCESSO p (NOLOCK)
        INNER JOIN OPERACAO o (NOLOCK) ON P.R_E_C_N_O_ = o.RECNO_PROCESSO
        WHERE 
            p.ATIVO = 'S' 
            AND o.CONDIC IN ('M','P','S')
            AND p.DESPEC IS NOT NULL 
            AND LTRIM(RTRIM(p.DESPEC)) <> ''
            AND ISNULL(o.EXECUT,0) > 0
    )
    SELECT
        code, codigo, componente, tipo, quantidade, unidade, custo_unitario, custo_total, recno_id
    FROM RankedBomItems
    WHERE rn = 1
    ORDER BY code ASC, codigo ASC;
`;

const queryBomItems_Cleanup = `
    WITH RankedBomItems AS (
        SELECT
            LTRIM(RTRIM(p.NUMPEC)) AS code, 
            ISNULL(LTRIM(RTRIM(o.NUMITE)),'') AS codigo,
            o.R_E_C_N_O_ AS recno_id,
            ROW_NUMBER() OVER (
                PARTITION BY LTRIM(RTRIM(p.NUMPEC)), ISNULL(LTRIM(RTRIM(o.NUMITE)),'')
                ORDER BY o.R_E_C_N_O_ DESC
            ) AS rn
        FROM dbo.PROCESSO p (NOLOCK)
        INNER JOIN OPERACAO o (NOLOCK) ON P.R_E_C_N_O_ = o.RECNO_PROCESSO
        WHERE 
            p.ATIVO = 'S' 
            AND o.CONDIC IN ('M','P','S')
            AND p.DESPEC IS NOT NULL 
            AND LTRIM(RTRIM(p.DESPEC)) <> ''
            AND ISNULL(o.EXECUT,0) > 0
    )
    SELECT
        code, codigo
    FROM RankedBomItems
    WHERE rn = 1
    ORDER BY code ASC, codigo ASC
`;

const listaDeTarefas = [
    {
        nomeEmpresa: `${clientPrefix}-BomItems-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryBomItems,
        rpcFunctionName: 'sincronizar_bom_items',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-BomItems-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryBomItems_Cleanup,
        rpcFunctionName: 'cleanup_bom_items',
        rpcParameterName: 'json_input_codes',
    },
];

async function migracaoBomItems() {
    const inicioMigracao = Date.now();
    let tarefasComFalha = 0;

    console.log(`\n=== INICIANDO MÓDULO: BOM ITEMS (${listaDeTarefas.length} passos) ===`);

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
        console.log(`⚠️ === MÓDULO BOM ITEMS FINALIZADO COM ERROS! === ⚠️`);
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");

        setTimeout(() => process.exit(0), 1000); 
    } else {
        console.log("🎉 === MÓDULO BOM ITEMS CONCLUÍDO COM SUCESSO! === 🎉");
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        setTimeout(() => process.exit(0), 1000);
    }
}

migracaoBomItems().catch(err => {
    console.error("ERRO NÃO TRATADO:", err);
    process.exit(0); 
});