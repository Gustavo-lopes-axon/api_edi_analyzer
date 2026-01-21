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
    console.log("Uso: node sync_products.js [nome_do_cliente]");
    process.exit(1);
}

const clientPrefix = clientKey.toUpperCase();
console.log(`--- INICIANDO PROCESSAMENTO PARA O CLIENTE: [${clientPrefix}] ---`);

initializeLogger(clientPrefix);

const BYTE_TO_MB = 1048576;
const mssqlConfig = getMssqlConfig(clientPrefix); 
const supabaseConfig = getSupabaseConfig(clientPrefix);

const queryProducts = `
    WITH RankedProducts AS (
        SELECT 
            LTRIM(RTRIM(p.NUMPEC)) AS code,
            COALESCE(NULLIF(LTRIM(RTRIM(p.DESPEC)), ''), '(Produto sem nome)') AS name,
            COALESCE(NULLIF(LTRIM(RTRIM(p.DESPEC)), ''), '(Produto sem descrição)') AS description,
            '9b823b9b-04f0-4d66-bf3b-3d3c55938054' AS category,
            'UN' AS unit,
            'Aço' AS material,
            REPLACE(CAST(ISNULL(p.peso, 0) AS VARCHAR(50)), ',', '.') AS weight,
            '0' AS cost, 
            '0' AS price,
            'active' AS status,
            REPLACE(CAST(ISNULL((
                SELECT SUM(ISNULL(M.LEADTIME,1)) 
                FROM PROCESSO P2 (NOLOCK)
                INNER JOIN OPERACAO O2 (NOLOCK) ON O2.RECNO_PROCESSO = P2.R_E_C_N_O_
                INNER JOIN MAQUINA M (NOLOCK) ON O2.MAQUIN = M.NUMMAQ
                WHERE P2.R_E_C_N_O_ = P.R_E_C_N_O_
            ), 1) AS VARCHAR(50)), ',', '.') as leadtime,
            p.R_E_C_N_O_ AS recno_id,
            ROW_NUMBER() OVER (PARTITION BY LTRIM(RTRIM(p.NUMPEC)) ORDER BY p.R_E_C_N_O_ DESC) as rn
        FROM dbo.PROCESSO AS p (NOLOCK)
        WHERE ATIVO ='S'
    )
    SELECT
        code, name, description, category, unit, material, 
        weight, cost, price, status, leadtime, recno_id
    FROM RankedProducts
    WHERE rn = 1
    ORDER BY code ASC
`;

const queryProducts_Cleanup = `
    SELECT 
        LTRIM(RTRIM(p.NUMPEC)) AS code
    FROM dbo.PROCESSO AS p (NOLOCK)
    WHERE ATIVO ='S'
    GROUP BY LTRIM(RTRIM(p.NUMPEC))
    ORDER BY LTRIM(RTRIM(p.NUMPEC)) ASC
`;

const listaDeTarefas = [
    {
        nomeEmpresa: `${clientPrefix}-Products-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryProducts,
        rpcFunctionName: 'sincronizar_products',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-Products-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryProducts_Cleanup,
        rpcFunctionName: 'cleanup_products',
        rpcParameterName: 'json_input_codes',
    },
];

async function migracaoProducts() {
    const inicioMigracao = Date.now();
    let tarefasComFalha = 0;

    console.log(`\n=== INICIANDO MÓDULO: PRODUCTS (${listaDeTarefas.length} passos) ===`);

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
        console.log(`⚠️ === MÓDULO PRODUCTS FALHOU! === ⚠️`);
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(1); 
    } else {
        console.log("🎉 === MÓDULO PRODUCTS CONCLUÍDO COM SUCESSO! === 🎉");
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(0);
    }
}

migracaoProducts();