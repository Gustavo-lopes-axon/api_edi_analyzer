require('dotenv').config();
const Firebird = require('node-firebird');
const { getFirebirdConfig } = require('./config.js');

const options = getFirebirdConfig('PEDERTRACTOR');

console.log(`🚀 Tentando conectar em: ${options.host}:${options.port}...`);

Firebird.attach(options, function(err, db) {
    if (err) {
        console.error('❌ ERRO DE CONEXÃO:');
        console.error('Mensagem:', err.message);
        console.log('-------------------------------------------');
        console.log('💡 Dica: Verifique se o IP está liberado no Firewall deles.');
        return;
    }

    console.log('✅ CONEXÃO ESTABELECIDA COM SUCESSO!');

    const testQuery = 'SELECT FIRST 5 * FROM VW_AXON_CAD_CLIENTE';
    
    db.query(testQuery, function(err, result) {
        if (err) {
            console.error('❌ ERRO NA CONSULTA (Permissão ou Sintaxe):', err.message);
        } else {
            console.log('✅ LEITURA DA VIEW OK!');
            console.log(`📊 Recebidos ${result.length} registros de teste.`);
            console.table(result);
        }

        db.detach();
        console.log('🔌 Conexão encerrada.');
    });
});