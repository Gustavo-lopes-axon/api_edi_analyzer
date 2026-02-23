const express = require("express");
const path = require("path");

const router = express.Router();

require("dotenv").config({ path: path.join(__dirname, "../../../../.env") });
const { executarQueryFirebird } = require("../../../../engines/firebirdClient.js");

const CLIENT_PREFIX = "PEDERTRACTOR";
/** View de notas fiscais recebidas (ajuste conforme o nome no Firebird) */
const VIEW_RECEBIMENTO = "VW_AXON_NF_RECEBIMENTO";

/** Nomes das colunas na view (ajuste conforme a view de recebimento) */
const COLS = {
  CUSTOMER_CODE: "CODIGO_CLIENTE",
  CUSTOMER_PN: "CODIGO_ITEM",
  ISSUE_DATE: "DATA_EMISSAO",
  NUMBER: "NUMERO",
  QTY: "QUANTIDADE",
  CANCELED: "CANCELADA",
  SUPPLIER_PN: "CODIGO_FORNECEDOR",
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

function rowToInvoice(row) {
  const str = (v) => (v === undefined || v === null ? "" : String(v));
  const num = (v) => (v === undefined || v === null ? 0 : Number(v));
  const rawDate = get(row, COLS.ISSUE_DATE, "ISSUEDATE", "DATAEMISSAO", "EMI");
  const d =
    rawDate != null
      ? rawDate instanceof Date
        ? rawDate.toISOString().slice(0, 10)
        : String(rawDate).slice(0, 10)
      : "";
  return {
    number: str(get(row, COLS.NUMBER, "NUMERO_NF", "NOTAFISCAL", "NF")),
    issueDate: d,
    qty: num(get(row, COLS.QTY, "QTD", "QTDE")),
  };
}

/**
 * GET /received-invoices
 * Parâmetros obrigatórios: customerCode, customerPN, startDate, endDate (YYYY-MM-DD).
 * Retorna ReceivedInvoiceResponse com "invoices" vazio se nenhuma nota for encontrada.
 * Não retorna notas canceladas.
 */
router.get("/", async (req, res) => {
  try {
    const customerCode = req.query.customerCode?.trim();
    const customerPN = req.query.customerPN?.trim();
    const startDate = req.query.startDate?.trim();
    const endDate = req.query.endDate?.trim();

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

    const dc = COLS.CUSTOMER_CODE;
    const dpn = COLS.CUSTOMER_PN;
    const ddate = COLS.ISSUE_DATE;
    const dcancel = COLS.CANCELED;

    const sql = `
      SELECT *
      FROM ${VIEW_RECEBIMENTO}
      WHERE TRIM(COALESCE(${dc}, '')) = '${sc}'
        AND TRIM(UPPER(COALESCE(${dpn}, ''))) = UPPER('${spn}')
        AND CAST(${ddate} AS DATE) >= CAST('${sd}' AS DATE)
        AND CAST(${ddate} AS DATE) <= CAST('${ed}' AS DATE)
        AND COALESCE(${dcancel}, 'N') <> 'S'
        AND COALESCE(${dcancel}, 'N') <> 'T'
      ORDER BY ${ddate} DESC, ${COLS.NUMBER} DESC
    `.trim();

    const rows = await executarQueryFirebird(CLIENT_PREFIX, sql);
    const invoices = (rows || []).map(rowToInvoice);

    const supplierPN = rows?.length
      ? String(get(rows[0], COLS.SUPPLIER_PN, "SUPPLIERPN", "CODIGO_FORN") ?? "").trim()
      : "";

    const data = {
      customerCode,
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
    console.error("GET /received-invoices (Firebird):", err.message);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: err.message,
    });
  }
});

module.exports = router;
