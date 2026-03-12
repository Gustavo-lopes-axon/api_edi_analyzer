const express = require("express");
const path = require("path");

const router = express.Router();

require("dotenv").config({ path: path.join(__dirname, "../../../../.env") });
const { executarQueryFirebird } = require("../../../../engines/firebirdClient.js");
const { getConnectionMySQL, executarQueryMySQL } = require("../../../../engines/mysqlClient.js");

const CLIENT_PREFIX = "PEDERTRACTOR";
/** View da última data firme por item (opcional; se não existir, retorna lastFirmDate null) */
const VIEW_LAST_FIRM_DATE = "VW_AXON_LAST_FIRM_DATE";

/**
 * Valida o corpo do POST /analysis.
 * Retorna { valid: true, body } ou { valid: false, message }.
 */
function validateAnalysisBody(body) {
  if (!body || typeof body !== "object") {
    return { valid: false, message: "Corpo da requisição deve ser um objeto JSON." };
  }

  const releaseId = body.releaseId ?? body.release_id;
  if (releaseId == null || String(releaseId).trim() === "") {
    return { valid: false, message: "releaseId é obrigatório." };
  }

  const force = body.force;
  if (typeof force !== "boolean") {
    return { valid: false, message: "force é obrigatório e deve ser um boolean." };
  }

  const analysisVersion = body.analysisVersion ?? body.analysis_version;
  if (analysisVersion === undefined || analysisVersion === null) {
    return { valid: false, message: "analysisVersion é obrigatório." };
  }
  const version = Number(analysisVersion);
  if (!Number.isInteger(version) || version < 0) {
    return { valid: false, message: "analysisVersion deve ser um inteiro não negativo." };
  }

  const analysisConfigs = body.analysisConfigs ?? body.analysis_configs;
  if (!analysisConfigs || typeof analysisConfigs !== "object" || Array.isArray(analysisConfigs)) {
    return { valid: false, message: "analysisConfigs é obrigatório e deve ser um objeto." };
  }

  return {
    valid: true,
    body: {
      releaseId: String(releaseId).trim(),
      force,
      analysisVersion: version,
      analysisConfigs,
    },
  };
}

/**
 * Gera um ID numérico para o registro de análise (persistência pode sobrescrever).
 */
function generateAnalysisRecordId() {
  return String(50000 + Math.floor(Math.random() * 50000));
}

/**
 * Gera um ID numérico para o registro de análise de item (ex.: 65753433).
 */
function generateItemAnalysisRecordId() {
  return String(65000000 + Math.floor(Math.random() * 1000000));
}

/**
 * Valida o corpo do POST /analysis/items (ItemAnalysis).
 * Aceita body direto ou body.itemAnalysis.
 */
function validateItemAnalysisBody(body) {
  const item = body?.itemAnalysis ?? body;
  if (!item || typeof item !== "object") {
    return { valid: false, message: "Corpo da requisição deve ser um objeto (itemAnalysis)." };
  }

  const releaseId = item.releaseId ?? item.release_id;
  if (releaseId == null || String(releaseId).trim() === "") {
    return { valid: false, message: "releaseId é obrigatório." };
  }

  const releaseAnalysisId = item.releaseAnalysisId ?? item.release_analysis_id;
  if (releaseAnalysisId == null || String(releaseAnalysisId).trim() === "") {
    return { valid: false, message: "releaseAnalysisId é obrigatório." };
  }

  const deliveries = item.deliveries;
  if (deliveries !== undefined && !Array.isArray(deliveries)) {
    return { valid: false, message: "deliveries deve ser um array (pode ser vazio)." };
  }

  const comments = item.comments;
  if (comments !== undefined && !Array.isArray(comments)) {
    return { valid: false, message: "comments deve ser um array (pode ser vazio)." };
  }

  return { valid: true, item };
}

/**
 * POST /analysis
 * Insere novo registro de análise para um release.
 * Corpo: { releaseId, force, analysisVersion, analysisConfigs } (todos obrigatórios).
 * Retorna 201 Created com InsertionResponse { recordId }.
 */
router.post("/", async (req, res) => {
  try {
    const validation = validateAnalysisBody(req.body);
    if (!validation.valid) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: validation.message,
      });
    }

    const { releaseId, force, analysisVersion, analysisConfigs } = validation.body;
    const cfg = analysisConfigs ?? {};

    // Resolve the internal release PK from customer_release_id
    const conn = await getConnectionMySQL(CLIENT_PREFIX);
    let recordId;
    try {
      const [[releaseRow]] = await conn.execute(
        `SELECT r.id
         FROM releases r
         INNER JOIN customers c ON c.id = r.customer_id
         WHERE r.id = ?
            OR r.customer_release_id = ?
            OR r.custom_id LIKE ?
            OR CONCAT(c.internal_code, r.customer_release_id) = ?
         LIMIT 1`,
        [releaseId, releaseId, `%|r:${releaseId}`, releaseId]
      );

      if (!releaseRow) {
        return res.status(400).json({
          success: false,
          error: "Parâmetros inválidos",
          message: `Release com releaseId "${releaseId}" não encontrado.`,
        });
      }

      const [result] = await conn.execute(
        `INSERT INTO release_analyses
           (release_id, \`force\`, analysis_version,
            firm_policy, custom_firm_days, accept_increment, accept_cut,
            accept_date_variation, transit_qty_policy, create_order_if_not_exists,
            auto_implement_analysis_result, use_leadtime, default_leadtime, use_receipt,
            analysis_configs_json, analysis_status)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'not_analyzed')`,
        [
          releaseRow.id,
          force ? 1 : 0,
          analysisVersion,
          cfg.firmPolicy ?? cfg.firm_policy ?? null,
          cfg.customFirmDays ?? cfg.custom_firm_days ?? null,
          cfg.acceptIncrement ?? cfg.accept_increment ?? null,
          cfg.acceptCut ?? cfg.accept_cut ?? null,
          cfg.acceptDateVariation ?? cfg.accept_date_variation ?? null,
          cfg.transitQtyPolicy ?? cfg.transit_qty_policy ?? null,
          cfg.createOrderIfNotExists ?? cfg.create_order_if_not_exists ?? null,
          cfg.autoImplementAnalysisResult ?? cfg.auto_implement_analysis_result ?? null,
          cfg.useLeadtime ?? cfg.use_leadtime ?? null,
          cfg.defaultLeadtime ?? cfg.default_leadtime ?? null,
          cfg.useReceipt ?? cfg.use_receipt ?? null,
          JSON.stringify(analysisConfigs),
        ]
      );

      recordId = String(result.insertId);
    } finally {
      conn.release();
    }

    return res.status(201).json({
      success: true,
      data: {
        recordId,
      },
    });
  } catch (err) {
    console.error("POST /analysis:", err.message);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: err.message,
    });
  }
});

/**
 * POST /analysis/items
 * Insere dados da análise de um item (ItemAnalysis).
 * Corpo: objeto itemAnalysis ou { itemAnalysis: { ... } } com releaseId, releaseAnalysisId obrigatórios.
 * Retorna 201 Created com InsertionResponse { recordId }.
 */
router.post("/items", async (req, res) => {
  try {
    const validation = validateItemAnalysisBody(req.body);
    if (!validation.valid) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: validation.message,
      });
    }

    const item = validation.item;
    const releaseAnalysisId = String(item.releaseAnalysisId ?? item.release_analysis_id ?? "").trim();
    const itemCustomId = String(item.customId ?? item.custom_id ?? "").trim();

    const toDate = (v) => {
      if (v == null) return null;
      const s = String(v).trim();
      return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : null;
    };
    const toNum = (v) => (v == null ? null : (isNaN(Number(v)) ? null : Number(v)));
    const toBool = (v) => (v == null ? null : (v ? 1 : 0));

    const conn = await getConnectionMySQL(CLIENT_PREFIX);
    let recordId;
    try {
      await conn.beginTransaction();

      // Upsert item
      const [itemResult] = await conn.execute(
        `INSERT INTO release_analysis_items
           (custom_id, release_analysis_id, sequence, customer_purchase_order, customer_pn,
            customer_technical_revision, supplier_pn, supplier_technical_revision,
            customer_acc_qty, supplier_acc_qty, transit_acc_qty,
            customer_last_invoice_number, supplier_last_invoice_number, transit_invoice_qty,
            backlog_firm_date, release_firm_date, backlog_firm_qty, release_firm_qty,
            release_previous_firm_qty, firm_qty_variation, variation_type,
            is_missing, is_uncorrelated, has_qty_less_than_min_order_qty,
            has_qty_not_multiple_of_min_order_qty, qty_left_add, excess_cut_qty,
            backlog_total_qty, release_total_qty, total_qty_variation,
            analysis_result_firm_date, analysis_result_firm_qty, analysis_result_total_qty,
            comments, is_implemented, implementation_comments)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
         ON DUPLICATE KEY UPDATE
           release_analysis_id             = VALUES(release_analysis_id),
           sequence                        = VALUES(sequence),
           customer_purchase_order         = VALUES(customer_purchase_order),
           customer_pn                     = VALUES(customer_pn),
           customer_technical_revision     = VALUES(customer_technical_revision),
           supplier_pn                     = VALUES(supplier_pn),
           supplier_technical_revision     = VALUES(supplier_technical_revision),
           customer_acc_qty                = VALUES(customer_acc_qty),
           supplier_acc_qty                = VALUES(supplier_acc_qty),
           transit_acc_qty                 = VALUES(transit_acc_qty),
           customer_last_invoice_number    = VALUES(customer_last_invoice_number),
           supplier_last_invoice_number    = VALUES(supplier_last_invoice_number),
           transit_invoice_qty             = VALUES(transit_invoice_qty),
           backlog_firm_date               = VALUES(backlog_firm_date),
           release_firm_date               = VALUES(release_firm_date),
           backlog_firm_qty                = VALUES(backlog_firm_qty),
           release_firm_qty                = VALUES(release_firm_qty),
           release_previous_firm_qty       = VALUES(release_previous_firm_qty),
           firm_qty_variation              = VALUES(firm_qty_variation),
           variation_type                  = VALUES(variation_type),
           is_missing                      = VALUES(is_missing),
           is_uncorrelated                 = VALUES(is_uncorrelated),
           has_qty_less_than_min_order_qty = VALUES(has_qty_less_than_min_order_qty),
           has_qty_not_multiple_of_min_order_qty = VALUES(has_qty_not_multiple_of_min_order_qty),
           qty_left_add                    = VALUES(qty_left_add),
           excess_cut_qty                  = VALUES(excess_cut_qty),
           backlog_total_qty               = VALUES(backlog_total_qty),
           release_total_qty               = VALUES(release_total_qty),
           total_qty_variation             = VALUES(total_qty_variation),
           analysis_result_firm_date       = VALUES(analysis_result_firm_date),
           analysis_result_firm_qty        = VALUES(analysis_result_firm_qty),
           analysis_result_total_qty       = VALUES(analysis_result_total_qty),
           comments                        = VALUES(comments),
           is_implemented                  = VALUES(is_implemented),
           implementation_comments         = VALUES(implementation_comments)`,
        [
          itemCustomId,
          releaseAnalysisId,
          toNum(item.sequence),
          item.customerPurchaseOrder ?? item.customer_purchase_order ?? null,
          item.customerPN ?? item.customer_pn ?? null,
          item.customerTechnicalRevision ?? item.customer_technical_revision ?? null,
          item.supplierPN ?? item.supplier_pn ?? null,
          item.supplierTechnicalRevision ?? item.supplier_technical_revision ?? null,
          toNum(item.customerAccQty ?? item.customer_acc_qty),
          toNum(item.supplierAccQty ?? item.supplier_acc_qty),
          toNum(item.transitAccQty ?? item.transit_acc_qty),
          item.customerLastInvoiceNumber ?? item.customer_last_invoice_number ?? null,
          item.supplierLastInvoiceNumber ?? item.supplier_last_invoice_number ?? null,
          toNum(item.transitInvoiceQty ?? item.transit_invoice_qty),
          toDate(item.backlogFirmDate ?? item.backlog_firm_date),
          toDate(item.releaseFirmDate ?? item.release_firm_date),
          toNum(item.backlogFirmQty ?? item.backlog_firm_qty),
          toNum(item.releaseFirmQty ?? item.release_firm_qty),
          toNum(item.releasePreviousFirmQty ?? item.release_previous_firm_qty),
          toNum(item.firmQtyVariation ?? item.firm_qty_variation),
          item.variationType ?? item.variation_type ?? null,
          toBool(item.isMissing ?? item.is_missing),
          toBool(item.isUncorrelated ?? item.is_uncorrelated),
          toBool(item.hasQtyLessThanMinOrderQty ?? item.has_qty_less_than_min_order_qty),
          toBool(item.hasQtyNotMultipleOfMinOrderQty ?? item.has_qty_not_multiple_of_min_order_qty),
          toNum(item.qtyLeftAdd ?? item.qty_left_add),
          toNum(item.excessCutQty ?? item.excess_cut_qty),
          toNum(item.backlogTotalQty ?? item.backlog_total_qty),
          toNum(item.releaseTotalQty ?? item.release_total_qty),
          toNum(item.totalQtyVariation ?? item.total_qty_variation),
          toDate(item.analysisResultFirmDate ?? item.analysis_result_firm_date),
          toNum(item.analysisResultFirmQty ?? item.analysis_result_firm_qty),
          toNum(item.analysisResultTotalQty ?? item.analysis_result_total_qty),
          Array.isArray(item.comments) ? JSON.stringify(item.comments) : null,
          toBool(item.isImplemented ?? item.is_implemented) ?? 0,
          Array.isArray(item.implementationComments ?? item.implementation_comments)
            ? JSON.stringify(item.implementationComments ?? item.implementation_comments)
            : null,
        ]
      );

      // Resolve the item PK (insertId on insert, or re-query on duplicate)
      let analysisItemId;
      if (itemResult.insertId && itemResult.insertId > 0) {
        analysisItemId = itemResult.insertId;
      } else {
        const [[existingRow]] = await conn.execute(
          "SELECT id FROM release_analysis_items WHERE custom_id = ?",
          [itemCustomId]
        );
        analysisItemId = existingRow.id;
      }
      recordId = String(analysisItemId);

      // Upsert deliveries
      const deliveries = Array.isArray(item.deliveries) ? item.deliveries : [];
      for (const d of deliveries) {
        const dCustomId = String(d.customId ?? d.custom_id ?? "").trim();
        await conn.execute(
          `INSERT INTO release_analysis_deliveries
             (custom_id, analysis_item_id, sequence, due_date, delivery_time,
              backlog_delivery_type, backlog_qty, backlog_acc_qty,
              release_delivery_type, release_qty, release_acc_qty,
              qty_variation, acc_qty_variation,
              analysis_result_delivery_type, analysis_result_qty, analysis_result_acc_qty,
              analysis_result_qty_variation, analysis_result_acc_qty_variation, comments)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON DUPLICATE KEY UPDATE
             analysis_item_id                  = VALUES(analysis_item_id),
             sequence                          = VALUES(sequence),
             due_date                          = VALUES(due_date),
             delivery_time                     = VALUES(delivery_time),
             backlog_delivery_type             = VALUES(backlog_delivery_type),
             backlog_qty                       = VALUES(backlog_qty),
             backlog_acc_qty                   = VALUES(backlog_acc_qty),
             release_delivery_type             = VALUES(release_delivery_type),
             release_qty                       = VALUES(release_qty),
             release_acc_qty                   = VALUES(release_acc_qty),
             qty_variation                     = VALUES(qty_variation),
             acc_qty_variation                 = VALUES(acc_qty_variation),
             analysis_result_delivery_type     = VALUES(analysis_result_delivery_type),
             analysis_result_qty               = VALUES(analysis_result_qty),
             analysis_result_acc_qty           = VALUES(analysis_result_acc_qty),
             analysis_result_qty_variation     = VALUES(analysis_result_qty_variation),
             analysis_result_acc_qty_variation = VALUES(analysis_result_acc_qty_variation),
             comments                          = VALUES(comments)`,
          [
            dCustomId,
            analysisItemId,
            toNum(d.sequence),
            toDate(d.dueDate ?? d.due_date),
            d.deliveryTime ?? d.delivery_time ?? null,
            d.backlogDeliveryType ?? d.backlog_delivery_type ?? null,
            toNum(d.backlogQty ?? d.backlog_qty),
            toNum(d.backlogAccQty ?? d.backlog_acc_qty),
            d.releaseDeliveryType ?? d.release_delivery_type ?? null,
            toNum(d.releaseQty ?? d.release_qty),
            toNum(d.releaseAccQty ?? d.release_acc_qty),
            toNum(d.qtyVariation ?? d.qty_variation),
            toNum(d.accQtyVariation ?? d.acc_qty_variation),
            d.analysisResultDeliveryType ?? d.analysis_result_delivery_type ?? null,
            toNum(d.analysisResultQty ?? d.analysis_result_qty),
            toNum(d.analysisResultAccQty ?? d.analysis_result_acc_qty),
            toNum(d.analysisResultQtyVariation ?? d.analysis_result_qty_variation),
            toNum(d.analysisResultAccQtyVariation ?? d.analysis_result_acc_qty_variation),
            Array.isArray(d.comments) ? JSON.stringify(d.comments) : null,
          ]
        );
      }

      await conn.commit();
    } catch (err) {
      try { await conn.rollback(); } catch (_) {}
      throw err;
    } finally {
      conn.release();
    }

    return res.status(201).json({
      success: true,
      data: {
        recordId,
      },
    });
  } catch (err) {
    console.error("POST /analysis/items:", err.message);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: err.message,
    });
  }
});

const CNPJ_DIGITS_REGEX = /^\d{14}$/;

/**
 * GET /analysis/items/last-firm-date
 * Busca a última data firme do item (customerCnpj + customerPurchaseOrder + customerPN).
 * Query obrigatórios: customerCnpj (14 dígitos), customerPurchaseOrder, customerPN.
 * Retorna LastFirmDateResponse com lastFirmDate vazio (null) se nenhum registro for encontrado.
 */
router.get("/items/last-firm-date", async (req, res) => {
  try {
    const customerCnpj = (req.query.customerCnpj ?? "").toString().replace(/\D/g, "").trim();
    const customerPurchaseOrder = (req.query.customerPurchaseOrder ?? req.query.customer_purchase_order ?? "").toString().trim();
    const customerPN = (req.query.customerPN ?? req.query.customer_pn ?? "").toString().trim();

    const missing = [];
    if (!customerCnpj) missing.push("customerCnpj");
    if (!customerPurchaseOrder) missing.push("customerPurchaseOrder");
    if (!customerPN) missing.push("customerPN");

    if (missing.length > 0) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: `Parâmetros obrigatórios: ${missing.join(", ")}`,
      });
    }

    if (!CNPJ_DIGITS_REGEX.test(customerCnpj)) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: "customerCnpj deve conter exatamente 14 dígitos (somente números).",
      });
    }

    let lastFirmDate = null;

    const rows = await executarQueryMySQL(
      CLIENT_PREFIX,
      `SELECT MAX(rai.analysis_result_firm_date) AS last_firm_date
       FROM release_analysis_items rai
       JOIN release_analyses      ra  ON ra.id  = rai.release_analysis_id
       JOIN releases               r   ON r.id   = ra.release_id
       JOIN customers              c   ON c.id   = r.customer_id
       WHERE c.cnpj                        = ?
         AND rai.customer_purchase_order   = ?
         AND rai.customer_pn               = ?`,
      [customerCnpj, customerPurchaseOrder, customerPN]
    );

    const raw = rows?.[0]?.last_firm_date;
    if (raw != null) {
      lastFirmDate = raw instanceof Date ? raw.toISOString().slice(0, 10) : String(raw).slice(0, 10);
    }

    const data = {
      customerCnpj,
      customerPurchaseOrder,
      customerPN,
      lastFirmDate,
    };

    return res.status(200).json({
      success: true,
      data,
    });
  } catch (err) {
    console.error("GET /analysis/items/last-firm-date:", err.message);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: err.message,
    });
  }
});

/** Valores aceitos para analysisStatus (estado da análise) */
const ANALYSIS_STATUS_VALUES = ["not_analyzed", "analyzing", "analyzed", "analysis_failed"];

/**
 * POST /analysis/status
 * Atualiza o status da análise de um release.
 * Corpo: { releaseAnalysisId, timestamp, analysisStatus } (todos obrigatórios).
 * Retorna 200 com UpdateResponse { numberOfRecordsUpdated }.
 */
router.post("/status", async (req, res) => {
  try {
    const body = req.body ?? {};
    const releaseAnalysisId = body.releaseAnalysisId ?? body.release_analysis_id;
    const timestamp = body.timestamp;
    const analysisStatus = body.analysisStatus ?? body.analysis_status;

    const missing = [];
    if (releaseAnalysisId == null || String(releaseAnalysisId).trim() === "") missing.push("releaseAnalysisId");
    if (timestamp == null || String(timestamp).trim() === "") missing.push("timestamp");
    if (analysisStatus == null || String(analysisStatus).trim() === "") missing.push("analysisStatus");

    if (missing.length > 0) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: `Campos obrigatórios: ${missing.join(", ")}`,
      });
    }

    const status = String(analysisStatus).trim().toLowerCase();
    if (!ANALYSIS_STATUS_VALUES.includes(status)) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: `analysisStatus deve ser um dos: ${ANALYSIS_STATUS_VALUES.join(", ")}`,
      });
    }

    const conn = await getConnectionMySQL(CLIENT_PREFIX);
    let numberOfRecordsUpdated;
    try {
      const [result] = await conn.execute(
        "UPDATE release_analyses SET analysis_status = ? WHERE id = ?",
        [status, String(releaseAnalysisId).trim()]
      );
      numberOfRecordsUpdated = result.affectedRows ?? 0;
    } finally {
      conn.release();
    }

    return res.status(200).json({
      success: true,
      data: {
        numberOfRecordsUpdated,
      },
    });
  } catch (err) {
    console.error("POST /analysis/status:", err.message);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: err.message,
    });
  }
});

/**
 * PUT /analysis/duration-and-totals
 * Atualiza duração e totais de uma análise (releaseAnalysisId).
 * Corpo: { releaseAnalysisId, analysisDuration, totals } (todos obrigatórios; totals = objeto AnalysisTotals).
 * Retorna 200 com UpdateResponse { numberOfRecordsUpdated }.
 */
router.put("/duration-and-totals", async (req, res) => {
  try {
    const body = req.body ?? {};
    const releaseAnalysisId = body.releaseAnalysisId ?? body.release_analysis_id;
    const analysisDuration = body.analysisDuration ?? body.analysis_duration;
    const totals = body.totals;

    const missing = [];
    if (releaseAnalysisId == null || String(releaseAnalysisId).trim() === "") missing.push("releaseAnalysisId");
    if (analysisDuration == null || String(analysisDuration).trim() === "") missing.push("analysisDuration");
    if (totals === undefined) missing.push("totals");

    if (missing.length > 0) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: `Campos obrigatórios: ${missing.join(", ")}`,
      });
    }

    if (typeof totals !== "object" || totals === null || Array.isArray(totals)) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: "totals deve ser um objeto (AnalysisTotals).",
      });
    }

    const conn = await getConnectionMySQL(CLIENT_PREFIX);
    let numberOfRecordsUpdated;
    try {
      const [result] = await conn.execute(
        "UPDATE release_analyses SET analysis_duration = ?, totals_json = ? WHERE id = ?",
        [
          String(analysisDuration).trim(),
          JSON.stringify(totals),
          String(releaseAnalysisId).trim(),
        ]
      );
      numberOfRecordsUpdated = result.affectedRows ?? 0;
    } finally {
      conn.release();
    }

    return res.status(200).json({
      success: true,
      data: {
        numberOfRecordsUpdated,
      },
    });
  } catch (err) {
    console.error("PUT /analysis/duration-and-totals:", err.message);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: err.message,
    });
  }
});

/**
 * PUT /analysis/items/implementation-comments
 * Atualiza os comentários de implementação de um item de análise.
 * Corpo: { itemAnalysisId, comments } (ambos obrigatórios; comments = array de strings).
 * Retorna 200 com UpdateResponse { numberOfRecordsUpdated }.
 */
router.put("/items/implementation-comments", async (req, res) => {
  try {
    const body = req.body ?? {};
    const itemAnalysisId = body.itemAnalysisId ?? body.item_analysis_id;
    const comments = body.comments;

    const missing = [];
    if (itemAnalysisId == null || String(itemAnalysisId).trim() === "") missing.push("itemAnalysisId");
    if (comments === undefined) missing.push("comments");

    if (missing.length > 0) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: `Campos obrigatórios: ${missing.join(", ")}`,
      });
    }

    if (!Array.isArray(comments)) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: "comments deve ser um array de strings.",
      });
    }

    const validComments = comments.every((c) => typeof c === "string");
    if (!validComments) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: "Cada elemento de comments deve ser uma string.",
      });
    }

    const conn = await getConnectionMySQL(CLIENT_PREFIX);
    let numberOfRecordsUpdated;
    try {
      const [result] = await conn.execute(
        "UPDATE release_analysis_items SET implementation_comments = ? WHERE id = ?",
        [JSON.stringify(comments), String(itemAnalysisId).trim()]
      );
      numberOfRecordsUpdated = result.affectedRows ?? 0;
    } finally {
      conn.release();
    }

    return res.status(200).json({
      success: true,
      data: {
        numberOfRecordsUpdated,
      },
    });
  } catch (err) {
    console.error("PUT /analysis/items/implementation-comments:", err.message);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: err.message,
    });
  }
});

module.exports = router;
