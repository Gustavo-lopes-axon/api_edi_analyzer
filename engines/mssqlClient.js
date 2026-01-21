const sql = require('mssql');
const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');

function sleep(ms) {
    return new Promise((res) => setTimeout(res, ms));
}

async function enviarLoteSeguro(supabase, rpcFunctionName, rpcParameterName, lote, logPrefix, nomeEmpresa, attempts = 3) {
    if (!lote || lote.length === 0) {
        return 0;
    }

    const loteBytes = Buffer.byteLength(JSON.stringify(lote), 'utf8');
    console.log(`${logPrefix}   -> Tentando enviar sub-lote de ${lote.length} registros (${loteBytes} bytes)...`);

    try {
        const rpcParams = { [rpcParameterName]: lote };
        const { data, error } = await supabase.rpc(rpcFunctionName, rpcParams);

        if (error) throw error; 

        console.log(`${logPrefix}   -> Sub-lote de ${lote.length} registros enviado com sucesso.`);
        return lote.length;

    } catch (err) {
        const errorMessage = (err.message || '').toString();

        const isNetworkError = errorMessage.includes('timeout') || 
                    errorMessage.includes('fetch failed') ||
                    errorMessage.includes('504: Gateway time-out') ||
                    errorMessage.includes('522: Connection timed out') ||
                    errorMessage.includes('502: Bad gateway') ||
                    errorMessage.includes('504') ||
                    errorMessage.includes('502') ||
                    errorMessage.includes('522') ||
                    errorMessage.includes('413') ||
                    errorMessage.toLowerCase().includes('payload') ||
                    errorMessage.toLowerCase().includes('request entity too large') ||
                    errorMessage.toLowerCase().includes(' payloadtoo') ||
                    errorMessage.toLowerCase().includes('econnreset') ||
                    errorMessage.toLowerCase().includes('econnaborted');

        const isLogicError = errorMessage.toLowerCase().includes('produto com código') || 
                                errorMessage.toLowerCase().includes('foreign key constraint') ||
                                errorMessage.toLowerCase().includes('violates unique constraint') ||
                                errorMessage.toLowerCase().includes('not found');


        if (isNetworkError || isLogicError) {
            if (lote.length > 1) {
                console.log(`${logPrefix}   -> Sub-lote de ${lote.length} falhou (erro de rede/payload/lógica). Dividindo pela metade...`);

                const metade1 = lote.slice(0, Math.ceil(lote.length / 2));
                const metade2 = lote.slice(Math.ceil(lote.length / 2));

                const sucesso1 = await enviarLoteSeguro(supabase, rpcFunctionName, rpcParameterName, metade1, logPrefix, nomeEmpresa, attempts);
                const sucesso2 = await enviarLoteSeguro(supabase, rpcFunctionName, rpcParameterName, metade2, logPrefix, nomeEmpresa, attempts);

                return sucesso1 + sucesso2;
            }

            if (isLogicError) {
                try {
                    const logsDir = './logs';
                    if (!fs.existsSync(logsDir)) fs.mkdirSync(logsDir, { recursive: true });

                    const filePath = `${logsDir}/failed_records_${nomeEmpresa.replace(/[^a-zA-Z0-9_-]/g,'')}.log`; 

                    const time = new Date().toISOString();
                    const line = `${time} | LOGIC_ERROR | ${errorMessage} | payload: ${JSON.stringify(lote[0])}\n`;
                    fs.appendFileSync(filePath, line, { encoding: 'utf8' });
                    console.error(`${logPrefix}   -> REGISTRO ÚNICO FALHOU POR ERRO DE LÓGICA: ${errorMessage}. Gravado em ${filePath}. Continuando.`);
                } catch (fileErr) {
                    console.error(`${logPrefix}   -> Falha ao gravar log de registro com erro lógico:`, fileErr.message || fileErr);
                }
                return 0;
            }

            if (attempts > 0) {
                const waitMs = (4 - attempts) * 1000 + 500;
                console.log(`${logPrefix}   -> Lote único falhou. Tentando novamente em ${waitMs}ms (${attempts} tentativas restantes)...`);
                await sleep(waitMs);
                return enviarLoteSeguro(supabase, rpcFunctionName, rpcParameterName, lote, logPrefix, nomeEmpresa, attempts - 1);
            }
        }

        console.error(`${logPrefix} Erro ao chamar RPC no Supabase (Lote ${lote.length}):`, err.message || err);
        throw err; 
    }
}

async function enviarLotesComFila(supabase, rpcFunctionName, rpcParameterName, dadosIniciais, tamanhoLoteInicial, logPrefix, nomeEmpresa) {
    const filaDeLotes = [];
    const totalRegistros = dadosIniciais.length;
    const totalLotesIniciais = Math.ceil(totalRegistros / tamanhoLoteInicial); 
    let registrosSucessoTotal = 0;

    console.log(`${logPrefix} Processando ${totalRegistros} registros em ${totalLotesIniciais} lotes de ${tamanhoLoteInicial} (tentativa inicial)...`);

    for (let i = 0; i < totalLotesIniciais; i++) {
        const inicio = i * tamanhoLoteInicial;
        const fim = inicio + tamanhoLoteInicial;
        filaDeLotes.push(dadosIniciais.slice(inicio, fim)); 
    }

    while (filaDeLotes.length > 0) {
        const loteAtual = filaDeLotes.shift();
        
        if (!loteAtual || loteAtual.length === 0) {
            continue;
        }

        const sucessoNoLote = await enviarLoteSeguro(supabase, rpcFunctionName, rpcParameterName, loteAtual, logPrefix, nomeEmpresa);
        registrosSucessoTotal += sucessoNoLote;
    }

    return registrosSucessoTotal;
}

async function iniciarSincronizacao(config) {
    const { 
        nomeEmpresa, mssqlConfig, supabaseConfig, 
        query, rpcFunctionName, rpcParameterName
    } = config;

    let pool;
    const logPrefix = `[${nomeEmpresa}]`;
    let totalRegistros = 0;
    let dadosEnviaveis;

    try {
        console.log(`${logPrefix} Iniciando...`);

        console.log(`${logPrefix} Conectando ao SQL Server: ${mssqlConfig.server}...`);
        pool = await sql.connect(mssqlConfig);
        console.log(`${logPrefix} Conectado. Executando query...`);
        
        const resultado = await pool.request().query(query);
        const dados = resultado.recordset;

        if (!dados || dados.length === 0) {
            console.log(`${logPrefix} 0 registros encontrados. Sincronização pulada.`);
            return 0;
        }

        dadosEnviaveis = dados;

        totalRegistros = dadosEnviaveis.length;
        let totalBytes = 0;
        try {
            totalBytes = dadosEnviaveis.reduce((acc, r) => acc + Buffer.byteLength(JSON.stringify(r), 'utf8'), 0);
        } catch (e) {
            totalBytes = 0;
        }

        console.log(`${logPrefix} Encontrados ${totalRegistros} registros no banco de origem (${totalBytes} bytes).`);

        console.log(`${logPrefix} Conectando ao Supabase...`);
        const supabase = createClient(supabaseConfig.url, supabaseConfig.key);

        const usaLotes = config.usaLotes === true;
        const isCleanupTask = rpcFunctionName.includes('cleanup');

        let tamanhoLoteAtual = totalRegistros;

        if (isCleanupTask) {
            tamanhoLoteAtual = Math.min(totalRegistros, 5000);
            console.log(`${logPrefix} Tarefa de limpeza: enviando até 5000 registros por lote (carga baixa).`);
        } else if (usaLotes && totalRegistros > 1) {
            try {
                let totalBytesForEstimate = totalBytes || 0;
                if (!totalBytesForEstimate) {
                    totalBytesForEstimate = dadosEnviaveis.reduce((acc, r) => acc + Buffer.byteLength(JSON.stringify(r), 'utf8'), 0);
                }
                const avgBytes = Math.max(1, Math.floor(totalBytesForEstimate / dadosEnviaveis.length));
                const MAX_PAYLOAD_BYTES = parseInt(process.env.MAX_PAYLOAD_BYTES || '2000000', 10);
                const estimatedBatch = Math.max(1, Math.floor(MAX_PAYLOAD_BYTES / avgBytes));
                const DEFAULT_BATCH_LIMIT = parseInt(process.env.DEFAULT_BATCH_LIMIT || '1000', 10);
                tamanhoLoteAtual = Math.min(totalRegistros, Math.max(1, Math.min(estimatedBatch, DEFAULT_BATCH_LIMIT)));

                console.log(`${logPrefix} Tamanho médio por registro: ${avgBytes} bytes. MAX payload: ${MAX_PAYLOAD_BYTES} bytes.`);
                console.log(`${logPrefix} Tamanho de lote inicial estimado: ${tamanhoLoteAtual} registros.`);
            } catch (errSize) {
                console.warn(`${logPrefix} Não foi possível estimar tamanho dos registros, usando lote completo:`, errSize.message || errSize);
                tamanhoLoteAtual = Math.min(totalRegistros, parseInt(process.env.DEFAULT_BATCH_LIMIT || '1000', 10));
            }
        } else if (!isCleanupTask) {
            console.log(`${logPrefix} Usando envio único (usaLotes=false) ou quantidade pequena.`);
        }

        const totalSucesso = await enviarLotesComFila(supabase, rpcFunctionName, rpcParameterName, dadosEnviaveis, tamanhoLoteAtual, logPrefix, nomeEmpresa);

        console.log(`${logPrefix} Sincronização concluída com sucesso! (${totalSucesso} de ${totalRegistros} processados)`);

        return { 
            totalRegistros: totalSucesso,
            totalOrigem: totalRegistros,
            totalBytes 
        };

    } catch (err) {
        console.error(`${logPrefix} ERRO FATAL no processo:`, err.message);
        throw err; 
    } finally {
        if (pool) {
            await pool.close();
            console.log(`${logPrefix} Conexão com SQL Server fechada.`);
        }
    }
}

module.exports = { iniciarSincronizacao };