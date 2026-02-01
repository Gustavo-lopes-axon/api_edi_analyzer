/**
 * Script para visualizar dados das views Axon no Firebird.
 *
 * Uso:
 *   node utils/view_firebird_views.js [cliente] [quantidade]
 *   node utils/view_firebird_views.js [cliente] [view_name] [quantidade]
 *
 * Exemplos:
 *   node utils/view_firebird_views.js martiaco
 *   node utils/view_firebird_views.js martiaco 5
 *   node utils/view_firebird_views.js martiaco VW_AXON_CAD_CLIENTE 10
 */

const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "../.env") });
const { executarQueryFirebird } = require("../engines/firebirdClient.js");

const VIEWS_DISPONIVEIS = [
  "VW_AXON_CAD_CLIENTE",
  "VW_AXON_CAD_ENG_ITEM",
  "VW_AXON_CAD_ENG_ESTRUTURA",
  "VW_AXON_CAD_ENG_ROTEIRO",
  "VW_AXON_FATURAMENTO",
  "VW_AXON_PEDIDO",
];

const LIMITE_PADRAO = 5;

function parseArgs() {
  const args = process.argv.slice(2);
  const cliente = args[0]?.toUpperCase();
  if (!cliente) {
    console.error(
      "Uso: node utils/view_firebird_views.js <cliente> [view_name] [quantidade]",
    );
    console.error("Exemplo: node utils/view_firebird_views.js martiaco");
    console.error(
      "Exemplo: node utils/view_firebird_views.js martiaco VW_AXON_CAD_CLIENTE 10",
    );
    process.exit(1);
  }

  const segundo = args[1];
  const terceiro = args[2];

  let viewFiltro = null;
  let limite = LIMITE_PADRAO;

  if (segundo) {
    const upper = segundo.toUpperCase();
    if (VIEWS_DISPONIVEIS.includes(upper)) {
      viewFiltro = upper;
      const num = parseInt(terceiro, 10);
      if (!Number.isNaN(num) && num > 0) limite = Math.min(num, 100);
    } else {
      const num = parseInt(segundo, 10);
      if (!Number.isNaN(num) && num > 0) limite = Math.min(num, 100);
    }
  }
  if (terceiro && viewFiltro) {
    const num = parseInt(terceiro, 10);
    if (!Number.isNaN(num) && num > 0) limite = Math.min(num, 100);
  }

  return { cliente, viewFiltro, limite };
}

function exibirRegistros(viewName, registros) {
  if (!registros || registros.length === 0) {
    console.log("  (nenhum registro)\n");
    return;
  }

  console.log(`  Total: ${registros.length} registro(s)\n`);

  // Firebird pode retornar chaves em UPPER; normalizar para exibição
  const primeiro = registros[0];
  const colunas = Object.keys(primeiro);

  const larguraCol = 20;
  const sep = colunas.map(() => "-".repeat(larguraCol)).join(" ");
  const header = colunas
    .map((c) => String(c).slice(0, larguraCol).padEnd(larguraCol))
    .join(" ");

  console.log("  " + header);
  console.log("  " + sep);

  for (const row of registros) {
    const line = colunas
      .map((col) => {
        let val = row[col];
        if (val === null || val === undefined) val = "";
        const str = String(val).slice(0, larguraCol).padEnd(larguraCol);
        return str;
      })
      .join(" ");
    console.log("  " + line);
  }
  console.log("");
}

async function consultarView(clientPrefix, viewName, limite) {
  // Firebird: FIRST n antes das colunas
  const sql = `SELECT FIRST ${limite} * FROM ${viewName}`;
  return executarQueryFirebird(clientPrefix, sql);
}

async function main() {
  const { cliente, viewFiltro, limite } = parseArgs();

  const views = viewFiltro ? [viewFiltro] : VIEWS_DISPONIVEIS;

  console.log("========================================");
  console.log(`Firebird – Visualização de views Axon`);
  console.log(`Cliente: ${cliente} | Limite: ${limite} por view`);
  console.log("========================================\n");

  for (const viewName of views) {
    console.log(`\n--- ${viewName} ---`);
    try {
      const registros = await consultarView(cliente, viewName, limite);
      exibirRegistros(viewName, registros);
    } catch (err) {
      console.error(`  Erro: ${err.message}\n`);
    }
  }

  console.log("========================================");
  console.log("Concluído.");
  console.log("========================================");
}

main().catch((err) => {
  console.error("Falha geral:", err.message);
  process.exit(1);
});
