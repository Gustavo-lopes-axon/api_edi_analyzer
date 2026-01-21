require('dotenv').config();
const Firebird = require('node-firebird');
const { getFirebirdConfig } = require('../engines/config.js');

const options = getFirebirdConfig('PEDERTRACTOR');
options.timeout = 30000; 

const views = [
    'VW_AXON_CAD_CLIENTE',
    'VW_AXON_CAD_ENG_ITEM',
    'VW_AXON_CAD_ENG_ESTRUTURA',
    'VW_AXON_CAD_ENG_ROTEIRO',
    'VW_AXON_FATURAMENTO',
    'VW_AXON_PEDIDO'
];

function queryPromise(db, sql) {
    return new Promise((resolve, reject) => {
        db.query(sql, (err, result) => {
            if (err) reject(err);
            else resolve(result);
        });
    });
}

async function scanViews() {
    console.log(`🚀 Iniciando Varredura Técnica - Sequencial`);
    
    Firebird.attach(options, async function(err, db) {
        if (err) {
            console.error('❌ Erro de conexão:', err.message);
            return;
        }

        console.log(`✅ Conectado. Analisando ${views.length} views...\n`);

        for (const viewName of views) {
            process.stdout.write(`🔍 Analisando ${viewName}... `);
            try {
                const result = await queryPromise(db, `SELECT FIRST 1 * FROM ${viewName}`);
                
                if (result.length === 0) {
                    console.log(`⚠️  Vazia.`);
                } else {
                    const colunas = Object.keys(result[0]);
                    console.log(`✅ OK! (${colunas.length} colunas)`);
                    console.log(`   📑 Campos: ${colunas.join(', ')}\n`);
                }
            } catch (error) {
                console.log(`❌ Erro: ${error.message}`);
            }
        }

        console.log(`--------------------------------------------------`);
        console.log(`✨ Varredura finalizada.`);
        db.detach();
    });
}

scanViews();