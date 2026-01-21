const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const { createClient } = require('@supabase/supabase-js');
const { getSupabaseConfig } = require('../engines/config');

const [,, tabela, colunaChave] = process.argv;

if (!tabela || !colunaChave) {
    console.log("\n❌ Erro: Faltam argumentos.");
    console.log("👉 Uso: node utils/auditoria.js [nome_da_tabela] [coluna_id]\n");
    process.exit(1);
}

async function verificarDuplicados() {
    const config = getSupabaseConfig('MARTIACO');
    const url = config.url;
    const key = config.key; 

    if (!url || !key) {
        console.error("❌ ERRO: URL ou KEY não encontradas.");
        console.log("No seu .env deve existir:");
        console.log("MARTIACO_SUPABASE_URL=...");
        console.log("MARTIACO_SUPABASE_KEY=...");
        return;
    }

    const supabase = createClient(url, key);

    console.log(`\n🔎 Auditoria: [${tabela}] | Coluna: [${colunaChave}]`);
    console.log("--------------------------------------------------");

    const { data, error } = await supabase
        .from(tabela)
        .select(colunaChave);

    if (error) {
        console.error("❌ Erro Supabase:", error.message);
        return;
    }

    const contagem = {};
    const duplicados = [];

    data.forEach(item => {
        const val = item[colunaChave];
        if (val) contagem[val] = (contagem[val] || 0) + 1;
    });

    for (const key in contagem) {
        if (contagem[key] > 1) {
            duplicados.push({ [colunaChave]: key, repeticoes: contagem[key] });
        }
    }

    if (duplicados.length > 0) {
        console.warn(`⚠️ Encontrados ${duplicados.length} duplicados!`);
        console.table(duplicados.slice(0, 20));
    } else {
        console.log("✅ Nenhum duplicado encontrado.");
    }
}

verificarDuplicados();