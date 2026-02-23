const express = require("express");
const path = require("path");

const router = express.Router();

require("dotenv").config({ path: path.join(__dirname, "../../../../.env") });
const {
  executarQueryFirebird,
} = require("../../../../engines/firebirdClient.js");

const CLIENT_PREFIX = "PEDERTRACTOR";
const VIEW_FATURAMENTO = "VW_AXON_FATURAMENTO";

/** Colunas da view VW_AXON_FATURAMENTO (conforme prints) */
const COLS = {
  COD_CLIENTE: "COD_CIENTE", //colocaram o nome errado, o certo seria COD_CLIENTE
  RAZAO_SOCIAL: "RAZAO_SOCIAL",
  DATA_EMISSAO: "DATA_EMISSAO",
  NOTA_FISCAL: "NOTA_FISCAL",
  SEQUENCIA: "SEQUENCIA",
  COD_ITEM: "COD_ITEM",
  DESCRICAO: "DESCRICAO",
  QUANTIDADE_FATURADA: "QUANTIDADE_FATURADA",
  SITUACAO_NFE: "SITUACAO_NFE",
  // Colunas que nao existem
  PEDIDO_COMPRA: "PEDIDO_COMPRA",
  COD_FORNECEDOR: "COD_FORNECEDOR",
};

const DATE_REGEX = /^\d{4}-\d{2}-\d{2}$/;

function escapeSql(value) {
  if (!value || typeof value !== "string") return "";
  return value.replace(/'/g, "''");
}

function get(row, ...keys) {
  for (const key of keys) {
    const k = String(key).toUpperCase();
    const v = row[key] ?? row[k];
    if (v !== undefined && v !== null) return v;
  }
  return undefined;
}

function formatDate(raw) {
  if (raw == null) return "";
  if (raw instanceof Date) return raw.toISOString().slice(0, 10);
  return String(raw).slice(0, 10);
}

function parseNum(v) {
  if (v === undefined || v === null) return 0;
  const n = Number(String(v).replace(/\s/g, "").replace(",", "."));
  return Number.isNaN(n) ? 0 : n;
}

/** Agrupa linhas por (NOTA_FISCAL, DATA_EMISSAO) e soma QUANTIDADE_FATURADA. */
function buildInvoicesArray(rows) {
  const byKey = new Map();
  for (const row of rows || []) {
    const number = String(get(row, COLS.NOTA_FISCAL) ?? "").trim();
    const issueDate = formatDate(get(row, COLS.DATA_EMISSAO));
    const qty = parseNum(get(row, COLS.QUANTIDADE_FATURADA));
    const key = `${number}|${issueDate}`;
    const prev = byKey.get(key);
    if (prev) prev.qty += qty;
    else byKey.set(key, { number, issueDate, qty });
  }
  const list = Array.from(byKey.values());
  list.sort((a, b) => {
    const d = (b.issueDate || "").localeCompare(a.issueDate || "");
    return d !== 0 ? d : (b.number || "").localeCompare(a.number || "");
  });
  return list;
}

/**
 * GET /issued-invoices
 * Parâmetros obrigatórios: customerCode, customerPN, startDate, endDate (YYYY-MM-DD).
 * Opcional: customerPurchaseOrder (não filtra na view; apenas ecoado na resposta).
 * Retorna IssuedInvoiceResponse com "invoices" vazio se nenhuma nota for encontrada.
 * Não retorna notas canceladas.
 */
router.get("/", async (req, res) => {
  try {
    const customerCode = req.query.customerCode?.trim();
    const customerPurchaseOrder = req.query.customerPurchaseOrder?.trim();
    const customerPN = req.query.customerPN?.trim();
    const startDate = req.query.startDate?.trim();
    const endDate = req.query.endDate?.trim();

    console.log("[issued-invoices] GET / chamado", {
      filtros: {
        customerCode,
        customerPurchaseOrder: customerPurchaseOrder ?? "(não informado)",
        customerPN,
        startDate,
        endDate,
      },
    });

    const missing = [];
    if (!customerCode) missing.push("customerCode");
    if (!customerPN) missing.push("customerPN");
    if (!startDate) missing.push("startDate");
    if (!endDate) missing.push("endDate");

    if (missing.length > 0) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: `Parâmetros obrigatórios: ${missing.join(", ")}`,
      });
    }

    if (!DATE_REGEX.test(startDate) || !DATE_REGEX.test(endDate)) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: "startDate e endDate devem estar no formato YYYY-MM-DD",
      });
    }

    const sc = escapeSql(customerCode);
    const spn = escapeSql(customerPN);
    const sd = escapeSql(startDate);
    const ed = escapeSql(endDate);

    const sql = `
      SELECT *
      FROM ${VIEW_FATURAMENTO}
      WHERE TRIM(COALESCE(${COLS.COD_CLIENTE}, '')) = '${sc}'
        AND TRIM(UPPER(COALESCE(${COLS.COD_ITEM}, ''))) = UPPER('${spn}')
        AND CAST(${COLS.DATA_EMISSAO} AS DATE) >= CAST('${sd}' AS DATE)
        AND CAST(${COLS.DATA_EMISSAO} AS DATE) <= CAST('${ed}' AS DATE)
        AND COALESCE(CAST(${COLS.SITUACAO_NFE} AS VARCHAR(10)), '') <> '90'
      ORDER BY ${COLS.DATA_EMISSAO} DESC, ${COLS.NOTA_FISCAL} DESC
    `.trim();

    const rows = await executarQueryFirebird(CLIENT_PREFIX, sql);
    const invoices = buildInvoicesArray(rows);

    const supplierPN = rows?.length
      ? String(get(rows[0], COLS.COD_FORNECEDOR) ?? "").trim()
      : "";

    const data = {
      customerCode,
      customerPurchaseOrder: customerPurchaseOrder ?? "",
      customerPN,
      startDate,
      endDate,
      supplierPN,
      invoices,
    };

    return res.status(200).json({
      success: true,
      data,
    });
  } catch (err) {
    console.error("GET /issued-invoices (Firebird):", err.message);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: err.message,
    });
  }
});

module.exports = router;
