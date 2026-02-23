const express = require("express");
const path = require("path");

const router = express.Router();

require("dotenv").config({ path: path.join(__dirname, "../../../../.env") });
const {
  executarQueryFirebird,
} = require("../../../../engines/firebirdClient.js");

const CLIENT_PREFIX = "PEDERTRACTOR";
const VIEW_CLIENTE = "VW_AXON_CAD_CLIENTE";

/** Nomes das colunas na view VW_AXON_CAD_CLIENTE */
const COLS = {
  CNPJ: "CNPJ",
  CODIGO: "COD_CLIENTE",
  RAZAO_SOCIAL: "RAZAO_SOCIAL",
  NOME_FANTASIA: "NOME_FANTASIA",
  ALIAS: "ALIAS",
  MUNICIPIO: "MUNICIPIO",
  UF: "UF",
  PAIS: "PAIS",
};

function get(row, key) {
  const v = row && row[key];
  return v != null ? String(v).trim() : "";
}

/**
 * Normaliza CNPJ para apenas dígitos.
 * @param {string} value
 * @returns {string}
 */
function cnpjApenasDigitos(value) {
  if (!value || typeof value !== "string") return "";
  return value.replace(/\D/g, "");
}

/**
 * Escapa aspas simples para uso em SQL (Firebird).
 * @param {string} value
 * @returns {string}
 */
function escapeSql(value) {
  if (!value || typeof value !== "string") return "";
  return value.replace(/'/g, "''");
}

/**
 * GET /customers/:cnpj
 * Retorna um customer pelo CNPJ, consultando direto na view Firebird VW_AXON_CAD_CLIENTE.
 */
router.get("/:cnpj", async (req, res) => {
  try {
    const cnpjParam = req.params.cnpj;
    if (!cnpjParam || !cnpjParam.trim()) {
      return res.status(400).json({ error: "CNPJ é obrigatório" });
    }

    const digits = cnpjApenasDigitos(cnpjParam);
    if (digits.length !== 14) {
      return res
        .status(400)
        .json({ error: "CNPJ deve conter 14 dígitos/Parâmetros inválidos" });
    }

    const safeDigits = escapeSql(digits);

    const sql = `
      SELECT FIRST 1 *
      FROM ${VIEW_CLIENTE}
      WHERE REPLACE(REPLACE(REPLACE(REPLACE(TRIM(COALESCE(${COLS.CNPJ}, '')), '.', ''), '/', ''), '-', ''), ' ', '') = '${safeDigits}'
    `.trim();

    const rows = await executarQueryFirebird(CLIENT_PREFIX, sql);

    if (!rows || rows.length === 0) {
      return res
        .status(404)
        .json({ success: false, error: "Cliente não encontrado" });
    }

    const row = rows[0];
    const data = {
      cnpj: digits || get(row, COLS.CNPJ) || "",
      internalCode: get(row, COLS.CODIGO),
      companyName: get(row, COLS.RAZAO_SOCIAL),
      tradeName: get(row, COLS.NOME_FANTASIA),
      alias: get(row, COLS.ALIAS),
      municipality: get(row, COLS.MUNICIPIO),
      state: get(row, COLS.UF),
      country: get(row, COLS.PAIS),
    };

    return res.json({ success: true, data });
  } catch (err) {
    console.error("GET /customers/:cnpj (Firebird):", err.message);
    return res
      .status(500)
      .json({ error: "Erro ao buscar cliente no Firebird/Erro no servidor" });
  }
});

module.exports = router;
