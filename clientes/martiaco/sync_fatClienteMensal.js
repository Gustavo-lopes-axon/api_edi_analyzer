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
    console.log("Uso: node sync_fatClienteMensal.js [nome_do_cliente]");
    process.exit(1);
}

const clientPrefix = clientKey.toUpperCase();
console.log(`--- INICIANDO PROCESSAMENTO PARA O CLIENTE: [${clientPrefix}] ---`);

initializeLogger(clientPrefix);

const BYTE_TO_MB = 1048576;
const mssqlConfig = getMssqlConfig(clientPrefix); 
const supabaseConfig = getSupabaseConfig(clientPrefix);

const queryFatClienteMensal = `
    SELECT 
    CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
        ISNULL(CAST(ANO AS VARCHAR), '0') + 
        ISNULL(CAST(MES AS VARCHAR), '0') + 
        ISNULL(CAST(DIA AS VARCHAR), '0') + 
        ISNULL(CLI, 'NA')
    ), 2) AS id, 
    ANO, MES, DIA, sum(tb.val) as valor_faturado , cli as cliente, MAX(tb.razao) as razao_cliente
    FROM ( 
        SELECT 
        YEAR(N.EMI) AS ANO, MONTH(N.EMI) AS MES, DAY(N.EMI) AS DIA, N.VALMERC AS VAL, N.CLI, N.RAZAO
        FROM NOTAFISC N (NOLOCK)     
        INNER JOIN CT_OPERACAO O (NOLOCK) ON O.CODIGO = N.CT_OPERACAO      
        WHERE N.SITUACAO ='IMPRESSA' 
        AND O.GERACONTAS ='S' AND N.NOTACLI ='S' AND N.CFOP NOT IN (2101,1201,1949,2949)
        AND N.CLI IS NOT NULL 
        ) AS TB      
    WHERE ANO = YEAR(GETDATE())      
    GROUP BY ANO,MES,DIA,CLI
    ORDER BY ano, mes, dia asc
`;

const listaDeTarefas = [
    {
        nomeEmpresa: `${clientPrefix}-FatClienteMensal-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryFatClienteMensal,
        rpcFunctionName: 'sincronizar_xdsh_fat_cliente_mensal',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
];

async function migracaoFatClienteMensal() {
    const inicioMigracao = Date.now();
    let tarefasComFalha = 0;

    console.log(`\n=== INICIANDO MÓDULO: FAT CLIENTE MENSAL (${listaDeTarefas.length} passos) ===`);

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
        console.log(`⚠️ === MÓDULO FAT CLIENTE MENSAL FALHOU! === ⚠️`);
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(1); 
    } else {
        console.log("🎉 === MÓDULO FAT CLIENTE MENSAL CONCLUÍDO COM SUCESSO! === 🎉");
        console.log(`⏱️ Tempo Total: ${tempoFormatado}`);
        console.log("=================================================");
        process.exit(0);
    }
}

migracaoFatClienteMensal();