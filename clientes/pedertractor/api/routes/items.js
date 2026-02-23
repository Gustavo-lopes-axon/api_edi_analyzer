const express = require("express");
const path = require("path");

const router = express.Router();

require("dotenv").config({ path: path.join(__dirname, "../../../../.env") });
const {
  executarQueryFirebird,
} = require("../../../../engines/firebirdClient.js");

const CLIENT_PREFIX = "PEDERTRACTOR";
const VIEW_ITEMS = "VW_AXON_CAD_ENG_ITEM";

/**
 * Colunas da view (COD_ITEM, PART_NUMBER, DESCRICAO).
 * Busca por PART_NUMBER = customerPN (código do item na visão da montadora).
 */
const COLS = {
  COD_ITEM: "COD_ITEM",
  PART_NUMBER: "PART_NUMBER",
  DESCRICAO: "DESCRICAO",
};

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

function str(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}
function num(v) {
  return v === undefined || v === null ? 0 : Number(v);
}

/**
 * Converte linha da view (COD_ITEM, PART_NUMBER, DESCRICAO) para ItemWithDetails.
 * Campos não presentes na view vêm como null ou valor padrão.
 */
function rowToItemWithDetails(row) {
  const partNumber = str(get(row, COLS.PART_NUMBER));
  const codItem = str(get(row, COLS.COD_ITEM));
  const descricao = str(get(row, COLS.DESCRICAO));

  return {
    customerCode: codItem || "",
    customerPN: partNumber,
    customerTechnicalRevision: "",
    supplierPN: "",
    supplierTechnicalRevision: "",
    lifecycleStage: "production",
    category: null,
    leadTime: 0,
    minOrderQty: 0,
    description: descricao || null,
  };
}

/**
 * GET /items?customerPN=1318581C1
 * Parâmetro obrigatório: customerPN (código do item na visão da montadora).
 * Retorna todos os itens encontrados (pode ser mais de um); o Analyzer avalia e adiciona comentários.
 */
router.get("/", async (req, res) => {
  try {
    const customerPN = req.query.customerPN;
    if (!customerPN || typeof customerPN !== "string" || !customerPN.trim()) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: "O parâmetro customerPN é obrigatório.",
      });
    }

    const safePN = escapeSql(customerPN.trim());
    const sql = `
      SELECT ${COLS.COD_ITEM}, ${COLS.PART_NUMBER}, ${COLS.DESCRICAO}
      FROM ${VIEW_ITEMS}
      WHERE TRIM(UPPER(COALESCE(${COLS.PART_NUMBER}, ''))) = UPPER('${safePN}')
    `.trim();

    const rows = await executarQueryFirebird(CLIENT_PREFIX, sql);

    const data = (rows || []).map(rowToItemWithDetails);

    return res.status(200).json({
      success: true,
      data,
    });
  } catch (err) {
    console.error("GET /items (Firebird):", err.message);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: err.message,
    });
  }
});

module.exports = router;
