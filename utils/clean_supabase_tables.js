require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const empresa = 'ARAMEBRAS';

const supabase = createClient(
  process.env[`SUPABASE_URL_${empresa}`],
  process.env[`SUPABASE_KEY_${empresa}`]
);

// Tabelas que fazem UPSERT no ETL (em ordem de dependência - reverter para DELETE)
const tablesToClean = [
  'sales_orders_history',
  'notas_fiscais_recebimento',
  'xdsh_fat_cliente_mensal',
  'sales_orders',
  'contas_receber',
  'contas_pagar',
  'purchase_orders',
  'production_orders',
  'routing_operations',
  'bom_items',
  'stock_items',
  'products',
  'suppliers',
  'customers',
];

(async () => {
  console.log('🧹 LIMPEZA DE TABELAS SUPABASE (VIA TRUNCATE)');
  console.log('='.repeat(60));
  console.log(`Empresa: ${empresa}`);
  console.log(`Tabelas a limpar: ${tablesToClean.length}`);
  console.log('='.repeat(60));
  console.log('');

  const results = {};

  for (const table of tablesToClean) {
    try {
      console.log(`[${table}] Deletando via RPC truncate...`);
      
      // Try using RPC truncate functions
      const rpcName = `truncate_${table}`;
      
      const { data, error } = await supabase.rpc(rpcName);
      
      if (error) {
        // Se RPC não existir, tenta TRUNCATE direto
        console.log(`  ℹ️  RPC não encontrado. Tentando SQL direto...`);
       
               // Special case for sales_orders - usar RPC específica
               if (table === 'sales_orders') {
                 const { error: rpcError } = await supabase.rpc('truncate_sales_orders_tables');
                 if (rpcError) {
                   console.error(`  ❌ RPC Error: ${rpcError.message}`);
                   results[table] = { status: 'ERRO', message: rpcError.message };
                 } else {
                   console.log(`  ✅ Truncado via RPC truncate_sales_orders_tables`);
                   results[table] = { status: 'LIMPO' };
                 }
               } else {
        
        const { error: sqlError } = await supabase
          .from(table)
          .delete()
          .gt('id', '00000000-0000-0000-0000-000000000000');  // Delete all UUIDs > null
        
        if (sqlError) {
          console.error(`  ❌ SQL Error: ${sqlError.message}`);
          results[table] = { status: 'ERRO', message: sqlError.message };
        } else {
          console.log(`  ✅ Deletado via SQL`);
          results[table] = { status: 'LIMPO' };
        }
        }
      } else {
        console.log(`  ✅ Truncado via RPC`);
        results[table] = { status: 'LIMPO' };
      }
    } catch (error) {
      console.error(`  ❌ Exceção: ${error.message}`);
      results[table] = { status: 'EXCEÇÃO', message: error.message };
    }
    
    // Small delay to avoid rate limiting
    await new Promise(r => setTimeout(r, 300));
  }

  console.log('');
  console.log('='.repeat(60));
  console.log('📊 RESUMO DA LIMPEZA');
  console.log('='.repeat(60));

  const summary = {
    limpo: 0,
    erro: 0,
    excecao: 0,
  };

  Object.entries(results).forEach(([table, result]) => {
    const icon = 
      result.status === 'LIMPO' ? '✅' :
      result.status === 'ERRO' ? '❌' :
      '💥';
    
    console.log(`${icon} ${table.padEnd(40)} ${result.status}`);
    
    if (result.status === 'LIMPO') summary.limpo++;
    else if (result.status === 'ERRO') summary.erro++;
    else summary.excecao++;
  });

  console.log('');
  console.log(`Total de tabelas:`);
  console.log(`  ✅ Limpas: ${summary.limpo}/${tablesToClean.length}`);
  console.log(`  ❌ Erros: ${summary.erro}`);
  console.log(`  💥 Exceções: ${summary.excecao}`);
  console.log('');

  if (summary.erro === 0 && summary.excecao === 0) {
    console.log('✨ Limpeza concluída com sucesso!');
    console.log('🚀 Pronto para rodar o teste limpo!');
    console.log('');
    console.log('Execute: node sincronizar.js aramebras');
  } else {
    console.log('⚠️  Alguns erros ocorreram. Verifique os detalhes acima.');
  }
})();
