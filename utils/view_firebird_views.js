/**
 * Script para visualizar dados das views Axon no Firebird.
 *
 * Uso:
 *   node utils/view_firebird_views.js <cliente> [quantidade]
 *   node utils/view_firebird_views.js <cliente> [view_name] [quantidade]
 *   node utils/view_firebird_views.js <cliente> geral   → resumo de todas as views (1 registro cada)
 *
 * Exemplos:
 *   node utils/view_firebird_views.js martiaco
 *   node utils/view_firebird_views.js pedertractor
 *   node utils/view_firebird_views.js pedertractor geral
 *   node utils/view_firebird_views.js pedertractor 5
 *   node utils/view_firebird_views.js pedertractor VW_AXON_CAD_CLIENTE 10
 */

const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "../.env") });
const { executarQueryFirebird } = require("../engines/firebirdClient.js");
const { getFirebirdConfig } = require("./config.js");

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
      "Uso: node utils/view_firebird_views.js <cliente> [geral | view_name] [quantidade]"
    );
    console.error("Exemplo: node utils/view_firebird_views.js pedertractor");
    console.error(
      "Exemplo: node utils/view_firebird_views.js pedertractor geral"
    );
    console.error(
      "Exemplo: node utils/view_firebird_views.js pedertractor VW_AXON_CAD_CLIENTE 10"
    );
    process.exit(1);
  }

  const segundo = args[1]?.toUpperCase();
  const terceiro = args[2];

  let viewFiltro = null;
  let limite = LIMITE_PADRAO;
  let modoGeral = segundo === "GERAL";

  if (segundo && !modoGeral) {
    if (VIEWS_DISPONIVEIS.includes(segundo)) {
      viewFiltro = segundo;
      const num = parseInt(terceiro, 10);
      if (!Number.isNaN(num) && num > 0) limite = Math.min(num, 100);
    } else {
      const num = parseInt(args[1], 10);
      if (!Number.isNaN(num) && num > 0) limite = Math.min(num, 100);
    }
  }
  if (terceiro && viewFiltro) {
    const num = parseInt(terceiro, 10);
    if (!Number.isNaN(num) && num > 0) limite = Math.min(num, 100);
  }

  if (modoGeral) limite = 1;

  return { cliente, viewFiltro, limite, modoGeral };
}

function exibirRegistros(viewName, registros, compact = false) {
  if (!registros || registros.length === 0) {
    console.log("  (nenhum registro)\n");
    return;
  }

  const primeiro = registros[0];
  const colunas = Object.keys(primeiro);

  if (compact) {
    // Modo geral: colunas + amostra de 1 registro em uma linha
    console.log(`  Colunas: ${colunas.join(", ")}`);
    const amostra = colunas
      .map((c) => {
        const v = primeiro[c];
        return `${c}=${
          v === null || v === undefined ? "" : String(v).slice(0, 15)
        }`;
      })
      .join(" | ");
    console.log(`  Amostra: ${amostra}\n`);
    return;
  }

  console.log(`  Total: ${registros.length} registro(s)\n`);

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
  const { cliente, viewFiltro, limite, modoGeral } = parseArgs();

  const cfg = getFirebirdConfig(cliente);
  const endereco = `${cfg.host}:${cfg.port}`;
  console.log("========================================");
  console.log(`Firebird – Visualização de views Axon`);
  console.log(
    `Cliente: ${cliente} | Limite: ${limite} por view${
      modoGeral ? " (geral)" : ""
    }`
  );
  console.log("========================================");
  console.log(`\n🔌 Testando conexão em ${endereco} ...`);

  try {
    await executarQueryFirebird(cliente, "SELECT 1 FROM RDB$DATABASE");
    console.log(`✅ Conexão OK com ${endereco}\n`);
  } catch (err) {
    console.error(`\n❌ Falha na conexão Firebird:`);
    console.error(`   ${err.message}`);
    console.error(
      `\n💡 Verifique: IP correto, firewall liberando a porta ${cfg.port}, e variáveis ${cliente}_FB_HOST, _FB_PORT, _FB_DATABASE no .env`
    );
    process.exit(1);
  }

  const views = viewFiltro ? [viewFiltro] : VIEWS_DISPONIVEIS;

  for (const viewName of views) {
    console.log(`--- ${viewName} ---`);
    try {
      const registros = await consultarView(cliente, viewName, limite);
      exibirRegistros(viewName, registros, modoGeral);
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
