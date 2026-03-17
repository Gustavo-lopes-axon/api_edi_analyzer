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
 * GET /analysis?releaseId=...
 * Lista análises de um release. Se releaseId não for informado, retorna lista vazia.
 * releaseId pode ser: id interno, customer_release_id ou recordId (internal_code + customer_release_id).
 * 200: SuccessResponse < AnalysisResponse >; analysis vazio se nenhuma análise encontrada ou se releaseId omitido.
 * 400: Parâmetros inválidos. 404: Release não encontrado. 500: Erro no servidor.
 */
router.get("/", async (req, res) => {
  try {
    const releaseId = (req.query.releaseId ?? req.query.release_id ?? "").toString().trim();
    if (!releaseId) {
      return res.status(200).json({
        success: true,
        data: {
          releaseId: "",
          analysis: [],
        },
      });
    }

    const conn = await getConnectionMySQL(CLIENT_PREFIX);
    let releasePk = null;
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
      releasePk = releaseRow?.id ?? null;
    } finally {
      conn.release();
    }

    if (releasePk == null) {
      return res.status(404).json({
        success: false,
        error: "Release not found",
        message: `Release with releaseId "${releaseId}" not found.`,
      });
    }

    const rows = await executarQueryMySQL(
      CLIENT_PREFIX,
      `SELECT id, release_id, analysis_version, analysis_status, analysis_duration,
              created_at, updated_at, totals_json, analysis_configs_json,
              firm_policy, custom_firm_days, accept_increment, accept_cut, accept_date_variation,
              transit_qty_policy, create_order_if_not_exists, auto_implement_analysis_result,
              use_leadtime, default_leadtime, use_receipt
       FROM release_analyses
       WHERE release_id = ?
       ORDER BY id DESC`,
      [releasePk]
    );

    const toIso = (v) => (v instanceof Date ? v.toISOString() : v != null ? String(v) : null);
    const defaultTotals = {
      analyzedItems: 0,
      validItems: 0,
      missingItems: 0,
      uncorrelatedItems: 0,
      itemsWithIncrementOnFirmPeriod: 0,
      itemsWithCutOnFirmPeriod: 0,
      itemsWithDateVariationOnFirmPeriod: 0,
      itemsWithoutVariationOnFirmPeriod: 0,
    };
    const analysis = (Array.isArray(rows) ? rows : []).map((row) => {
      let totals = defaultTotals;
      if (row.totals_json) {
        try {
          const parsed = typeof row.totals_json === "string" ? JSON.parse(row.totals_json) : row.totals_json;
          if (parsed && typeof parsed === "object") {
            totals = { ...defaultTotals, ...parsed };
          }
        } catch (_) {}
      }
      let analysisConfigs = {
        firmPolicy: row.firm_policy ?? null,
        customFirmDays: row.custom_firm_days != null ? Number(row.custom_firm_days) : null,
        acceptIncrement: row.accept_increment != null ? Boolean(row.accept_increment) : null,
        acceptCut: row.accept_cut != null ? Boolean(row.accept_cut) : null,
        acceptDateVariation: row.accept_date_variation != null ? Boolean(row.accept_date_variation) : null,
        transitQtyPolicy: row.transit_qty_policy ?? null,
        createOrderIfNotExists: row.create_order_if_not_exists != null ? Boolean(row.create_order_if_not_exists) : null,
        autoImplementAnalysisResult: row.auto_implement_analysis_result != null ? Boolean(row.auto_implement_analysis_result) : null,
        useLeadtime: row.use_leadtime != null ? Boolean(row.use_leadtime) : null,
        defaultLeadtime: row.default_leadtime != null ? Number(row.default_leadtime) : null,
        useReceipt: row.use_receipt != null ? Boolean(row.use_receipt) : null,
      };
      if (row.analysis_configs_json) {
        try {
          const parsed = typeof row.analysis_configs_json === "string" ? JSON.parse(row.analysis_configs_json) : row.analysis_configs_json;
          if (parsed && typeof parsed === "object") {
            analysisConfigs = {
              firmPolicy: parsed.firmPolicy ?? parsed.firm_policy ?? analysisConfigs.firmPolicy,
              customFirmDays: parsed.customFirmDays ?? parsed.custom_firm_days ?? analysisConfigs.customFirmDays,
              acceptIncrement: parsed.acceptIncrement ?? parsed.accept_increment ?? analysisConfigs.acceptIncrement,
              acceptCut: parsed.acceptCut ?? parsed.accept_cut ?? analysisConfigs.acceptCut,
              acceptDateVariation: parsed.acceptDateVariation ?? parsed.accept_date_variation ?? analysisConfigs.acceptDateVariation,
              transitQtyPolicy: parsed.transitQtyPolicy ?? parsed.transit_qty_policy ?? analysisConfigs.transitQtyPolicy,
              createOrderIfNotExists: parsed.createOrderIfNotExists ?? parsed.create_order_if_not_exists ?? analysisConfigs.createOrderIfNotExists,
              autoImplementAnalysisResult: parsed.autoImplementAnalysisResult ?? parsed.auto_implement_analysis_result ?? analysisConfigs.autoImplementAnalysisResult,
              useLeadtime: parsed.useLeadtime ?? parsed.use_leadtime ?? analysisConfigs.useLeadtime,
              defaultLeadtime: parsed.defaultLeadtime ?? parsed.default_leadtime ?? analysisConfigs.defaultLeadtime,
              useReceipt: parsed.useReceipt ?? parsed.use_receipt ?? analysisConfigs.useReceipt,
            };
          }
        } catch (_) {}
      }
      return {
        releaseAnalysisId: String(row.id),
        analysisVersion: Number(row.analysis_version) || 1,
        totals: {
          analyzedItems: Number(totals.analyzedItems) || 0,
          validItems: Number(totals.validItems) || 0,
          missingItems: Number(totals.missingItems) || 0,
          uncorrelatedItems: Number(totals.uncorrelatedItems) || 0,
          itemsWithIncrementOnFirmPeriod: Number(totals.itemsWithIncrementOnFirmPeriod) || 0,
          itemsWithCutOnFirmPeriod: Number(totals.itemsWithCutOnFirmPeriod) || 0,
          itemsWithDateVariationOnFirmPeriod: Number(totals.itemsWithDateVariationOnFirmPeriod) || 0,
          itemsWithoutVariationOnFirmPeriod: Number(totals.itemsWithoutVariationOnFirmPeriod) || 0,
        },
        analysisStatus: row.analysis_status ?? "not_analyzed",
        processingType: "auto",
        startProcessingTimestamp: toIso(row.created_at),
        endProcessingTimestamp: toIso(row.updated_at),
        analysisConfigs,
      };
    });

    return res.status(200).json({
      success: true,
      data: {
        releaseId: String(releaseId),
        analysis,
      },
    });
  } catch (err) {
    const msg = err && (err.message || err.code || String(err));
    console.error("GET /analysis:", msg);
    if (err && err.stack) console.error(err.stack);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: msg || "Erro desconhecido",
    });
  }
});

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

/**
 * GET /analysis/:releaseAnalysisId/header
 * Retorna o cabeçalho da análise (AnalysisHeader).
 * 200: SuccessResponse < AnalysisHeader >
 * 400: releaseAnalysisId inválido (ErrorResponse)
 * 404: Análise não encontrada (NullResponse: success true, data null)
 * 500: Erro no servidor (ErrorResponse)
 */
router.get("/:releaseAnalysisId/header", async (req, res) => {
  try {
    const releaseAnalysisIdRaw = (req.params.releaseAnalysisId ?? "").toString().trim();
    if (!releaseAnalysisIdRaw) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "releaseAnalysisId é obrigatório.",
      });
    }
    const releaseAnalysisIdNum = parseInt(releaseAnalysisIdRaw, 10);
    if (Number.isNaN(releaseAnalysisIdNum) || releaseAnalysisIdNum < 1 || String(releaseAnalysisIdNum) !== releaseAnalysisIdRaw) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "releaseAnalysisId deve ser um identificador válido da análise.",
      });
    }

    const rows = await executarQueryMySQL(
      CLIENT_PREFIX,
      `SELECT id, analysis_version, analysis_status, analysis_duration,
              created_at, updated_at, totals_json, analysis_configs_json,
              firm_policy, custom_firm_days, accept_increment, accept_cut, accept_date_variation,
              transit_qty_policy, create_order_if_not_exists, auto_implement_analysis_result,
              use_leadtime, default_leadtime, use_receipt
       FROM release_analyses
       WHERE id = ?
       LIMIT 1`,
      [releaseAnalysisIdNum]
    );

    const row = Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
    if (!row) {
      return res.status(404).json({
        success: true,
        data: null,
      });
    }

    const toIso = (v) => (v instanceof Date ? v.toISOString() : v != null ? String(v) : null);
    const defaultTotals = {
      analyzedItems: 0,
      validItems: 0,
      missingItems: 0,
      uncorrelatedItems: 0,
      itemsWithIncrementOnFirmPeriod: 0,
      itemsWithCutOnFirmPeriod: 0,
      itemsWithDateVariationOnFirmPeriod: 0,
      itemsWithoutVariationOnFirmPeriod: 0,
    };
    let totals = defaultTotals;
    if (row.totals_json) {
      try {
        const parsed = typeof row.totals_json === "string" ? JSON.parse(row.totals_json) : row.totals_json;
        if (parsed && typeof parsed === "object") {
          totals = { ...defaultTotals, ...parsed };
        }
      } catch (_) {}
    }
    let analysisConfigs = {
      firmPolicy: row.firm_policy ?? null,
      customFirmDays: row.custom_firm_days != null ? Number(row.custom_firm_days) : null,
      acceptIncrement: row.accept_increment != null ? Boolean(row.accept_increment) : null,
      acceptCut: row.accept_cut != null ? Boolean(row.accept_cut) : null,
      acceptDateVariation: row.accept_date_variation != null ? Boolean(row.accept_date_variation) : null,
      transitQtyPolicy: row.transit_qty_policy ?? null,
      createOrderIfNotExists: row.create_order_if_not_exists != null ? Boolean(row.create_order_if_not_exists) : null,
      autoImplementAnalysisResult: row.auto_implement_analysis_result != null ? Boolean(row.auto_implement_analysis_result) : null,
      useLeadtime: row.use_leadtime != null ? Boolean(row.use_leadtime) : null,
      defaultLeadtime: row.default_leadtime != null ? Number(row.default_leadtime) : null,
      useReceipt: row.use_receipt != null ? Boolean(row.use_receipt) : null,
    };
    if (row.analysis_configs_json) {
      try {
        const parsed = typeof row.analysis_configs_json === "string" ? JSON.parse(row.analysis_configs_json) : row.analysis_configs_json;
        if (parsed && typeof parsed === "object") {
          analysisConfigs = {
            firmPolicy: parsed.firmPolicy ?? parsed.firm_policy ?? analysisConfigs.firmPolicy,
            customFirmDays: parsed.customFirmDays ?? parsed.custom_firm_days ?? analysisConfigs.customFirmDays,
            acceptIncrement: parsed.acceptIncrement ?? parsed.accept_increment ?? analysisConfigs.acceptIncrement,
            acceptCut: parsed.acceptCut ?? parsed.accept_cut ?? analysisConfigs.acceptCut,
            acceptDateVariation: parsed.acceptDateVariation ?? parsed.accept_date_variation ?? analysisConfigs.acceptDateVariation,
            transitQtyPolicy: parsed.transitQtyPolicy ?? parsed.transit_qty_policy ?? analysisConfigs.transitQtyPolicy,
            createOrderIfNotExists: parsed.createOrderIfNotExists ?? parsed.create_order_if_not_exists ?? analysisConfigs.createOrderIfNotExists,
            autoImplementAnalysisResult: parsed.autoImplementAnalysisResult ?? parsed.auto_implement_analysis_result ?? analysisConfigs.autoImplementAnalysisResult,
            useLeadtime: parsed.useLeadtime ?? parsed.use_leadtime ?? analysisConfigs.useLeadtime,
            defaultLeadtime: parsed.defaultLeadtime ?? parsed.default_leadtime ?? analysisConfigs.defaultLeadtime,
            useReceipt: parsed.useReceipt ?? parsed.use_receipt ?? analysisConfigs.useReceipt,
          };
        }
      } catch (_) {}
    }

    const data = {
      releaseAnalysisId: String(row.id),
      analysisVersion: Number(row.analysis_version) || 1,
      totals: {
        analyzedItems: Number(totals.analyzedItems) || 0,
        validItems: Number(totals.validItems) || 0,
        missingItems: Number(totals.missingItems) || 0,
        uncorrelatedItems: Number(totals.uncorrelatedItems) || 0,
        itemsWithIncrementOnFirmPeriod: Number(totals.itemsWithIncrementOnFirmPeriod) || 0,
        itemsWithCutOnFirmPeriod: Number(totals.itemsWithCutOnFirmPeriod) || 0,
        itemsWithDateVariationOnFirmPeriod: Number(totals.itemsWithDateVariationOnFirmPeriod) || 0,
        itemsWithoutVariationOnFirmPeriod: Number(totals.itemsWithoutVariationOnFirmPeriod) || 0,
      },
      analysisStatus: row.analysis_status ?? "not_analyzed",
      processingType: "auto",
      startProcessingTimestamp: toIso(row.created_at),
      endProcessingTimestamp: toIso(row.updated_at),
      analysisConfigs,
    };

    return res.status(200).json({
      success: true,
      data,
    });
  } catch (err) {
    const msg = err && (err.message || err.code || String(err));
    console.error("GET /analysis/:releaseAnalysisId/header:", msg);
    if (err && err.stack) console.error(err.stack);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: msg || "Erro desconhecido",
    });
  }
});

/**
 * GET /analysis/:releaseAnalysisId/items-with-deliveries
 * Lista itens da análise com entregas (paginação e filtros).
 * Query: page, pageSize, sort (+supplierPN|+customerPN|+customerPurchaseOrder), filterByVariationType, problem, moqIssue, supplierPN, customerPN, customerPurchaseOrder, isImplemented.
 * 200: SuccessResponse < PaginatedResponse < ItemAnalysis, ItemAnalysisFilterParams >>
 * 400: releaseAnalysisId inválido (ErrorResponse). 500: Erro no servidor (ErrorResponse).
 */
router.get("/:releaseAnalysisId/items-with-deliveries", async (req, res) => {
  try {
    const releaseAnalysisIdRaw = (req.params.releaseAnalysisId ?? "").toString().trim();
    if (!releaseAnalysisIdRaw) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "releaseAnalysisId é obrigatório.",
      });
    }
    const releaseAnalysisIdNum = parseInt(releaseAnalysisIdRaw, 10);
    if (Number.isNaN(releaseAnalysisIdNum) || releaseAnalysisIdNum < 1 || String(releaseAnalysisIdNum) !== releaseAnalysisIdRaw) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "releaseAnalysisId deve ser um identificador válido da análise.",
      });
    }

    const query = req.query || {};
    const page = Math.max(1, parseInt(query.page, 10) || 1);
    const pageSize = Math.min(100, Math.max(1, parseInt(query.pageSize, 10) || 30));
    const sortParam = String(query.sort ?? "+supplierPN").trim() || "+supplierPN";
    const filterByVariationTypeRaw = (query.filterByVariationType ?? "").toString().trim();
    const problemRaw = (query.problem ?? "").toString().trim();
    const moqIssueRaw = (query.moqIssue ?? "").toString().trim();
    const supplierPNFilter = (query.supplierPN ?? "").toString().trim();
    const customerPNFilter = (query.customerPN ?? "").toString().trim();
    const customerPurchaseOrderFilter = (query.customerPurchaseOrder ?? "").toString().trim();
    const isImplementedRaw = (query.isImplemented ?? "").toString().trim().toLowerCase();

    const conditions = ["rai.release_analysis_id = ?"];
    const countParams = [releaseAnalysisIdNum];

    const variationTypes = filterByVariationTypeRaw.split(",").map((s) => s.trim()).filter((s) => ["cut", "increment", "date_variation"].includes(s));
    if (variationTypes.length > 0) {
      conditions.push(`rai.variation_type IN (${variationTypes.map(() => "?").join(",")})`);
      countParams.push(...variationTypes);
    }

    const problems = problemRaw.split(",").map((s) => s.trim()).filter((s) => ["missing", "uncorrelated"].includes(s));
    if (problems.length === 1) {
      if (problems[0] === "missing") conditions.push("rai.is_missing = 1");
      else conditions.push("rai.is_uncorrelated = 1");
    } else if (problems.length === 2) {
      conditions.push("(rai.is_missing = 1 OR rai.is_uncorrelated = 1)");
    }

    const moqIssues = moqIssueRaw.split(",").map((s) => s.trim()).filter((s) => ["less", "not_multiple"].includes(s));
    if (moqIssues.length === 1) {
      if (moqIssues[0] === "less") conditions.push("rai.has_qty_less_than_min_order_qty = 1");
      else conditions.push("rai.has_qty_not_multiple_of_min_order_qty = 1");
    } else if (moqIssues.length === 2) {
      conditions.push("(rai.has_qty_less_than_min_order_qty = 1 OR rai.has_qty_not_multiple_of_min_order_qty = 1)");
    }

    if (supplierPNFilter) {
      conditions.push("rai.supplier_pn = ?");
      countParams.push(supplierPNFilter);
    }
    if (customerPNFilter) {
      conditions.push("rai.customer_pn = ?");
      countParams.push(customerPNFilter);
    }
    if (customerPurchaseOrderFilter) {
      conditions.push("rai.customer_purchase_order = ?");
      countParams.push(customerPurchaseOrderFilter);
    }
    if (isImplementedRaw === "true" || isImplementedRaw === "false") {
      conditions.push("rai.is_implemented = ?");
      countParams.push(isImplementedRaw === "true" ? 1 : 0);
    }

    const whereClause = `WHERE ${conditions.join(" AND ")}`;
    const sortMap = { supplierPN: "rai.supplier_pn", customerPN: "rai.customer_pn", customerPurchaseOrder: "rai.customer_purchase_order" };
    const isDesc = sortParam.startsWith("-");
    const sortField = (isDesc ? sortParam.slice(1) : sortParam.replace(/^\+/, "")).trim() || "supplierPN";
    const orderBy = sortMap[sortField] || "rai.supplier_pn";
    const orderDir = isDesc ? "DESC" : "ASC";
    const offset = Math.max(0, (page - 1) * pageSize);
    const limitNum = Math.min(100, pageSize);

    const countRows = await executarQueryMySQL(
      CLIENT_PREFIX,
      `SELECT COUNT(*) AS total FROM release_analysis_items rai ${whereClause}`,
      countParams
    );
    const totalRecords = Number(countRows?.[0]?.total ?? 0);

    const itemRows = await executarQueryMySQL(
      CLIENT_PREFIX,
      `SELECT rai.id, rai.sequence, rai.customer_purchase_order, rai.customer_pn, rai.customer_technical_revision,
              rai.supplier_pn, rai.supplier_technical_revision, rai.customer_acc_qty, rai.supplier_acc_qty, rai.transit_acc_qty,
              rai.customer_last_invoice_number, rai.supplier_last_invoice_number, rai.transit_invoice_qty,
              rai.backlog_firm_date, rai.release_firm_date, rai.backlog_firm_qty, rai.release_firm_qty, rai.release_previous_firm_qty,
              rai.firm_qty_variation, rai.variation_type, rai.is_missing, rai.is_uncorrelated,
              rai.has_qty_less_than_min_order_qty, rai.has_qty_not_multiple_of_min_order_qty,
              rai.backlog_total_qty, rai.release_total_qty, rai.total_qty_variation,
              rai.analysis_result_firm_date, rai.analysis_result_firm_qty, rai.analysis_result_total_qty,
              rai.comments, rai.is_implemented, rai.implementation_comments
       FROM release_analysis_items rai
       ${whereClause}
       ORDER BY ${orderBy} ${orderDir}
       LIMIT ${limitNum} OFFSET ${offset}`,
      countParams
    );

    const itemList = Array.isArray(itemRows) ? itemRows : [];
    const itemIds = itemList.map((r) => r.id).filter((id) => id != null);
    let deliveriesByItem = {};
    if (itemIds.length > 0) {
      const placeholders = itemIds.map(() => "?").join(",");
      const delRows = await executarQueryMySQL(
        CLIENT_PREFIX,
        `SELECT analysis_item_id, id, sequence, due_date, delivery_time,
                backlog_delivery_type, backlog_qty, backlog_acc_qty, release_delivery_type, release_qty, release_acc_qty,
                qty_variation, acc_qty_variation, analysis_result_delivery_type, analysis_result_qty, analysis_result_acc_qty,
                analysis_result_qty_variation, analysis_result_acc_qty_variation, comments
         FROM release_analysis_deliveries
         WHERE analysis_item_id IN (${placeholders})
         ORDER BY analysis_item_id, sequence`,
        itemIds
      );
      const delList = Array.isArray(delRows) ? delRows : [];
      for (const d of delList) {
        const key = String(d.analysis_item_id);
        if (!deliveriesByItem[key]) deliveriesByItem[key] = [];
        deliveriesByItem[key].push(d);
      }
    }

    const toDateStr = (v) => (v instanceof Date ? v.toISOString().slice(0, 10) : v != null ? String(v).slice(0, 10) : null);
    const toTimeStr = (v) => (v == null ? null : typeof v === "string" ? v : v instanceof Date ? v.toTimeString().slice(0, 8) : String(v));
    const parseJsonArray = (v) => {
      if (v == null) return null;
      if (Array.isArray(v)) return v;
      if (typeof v === "string") {
        try {
          const p = JSON.parse(v);
          return Array.isArray(p) ? p : null;
        } catch (_) { return null; }
        }
      return null;
    };

    const records = itemList.map((row) => {
      const deliveries = deliveriesByItem[String(row.id)] || [];
      return {
        itemAnalysisId: String(row.id),
        sequence: row.sequence != null ? Number(row.sequence) : null,
        customerPurchaseOrder: row.customer_purchase_order ?? null,
        customerPN: row.customer_pn ?? null,
        customerTechnicalRevision: row.customer_technical_revision ?? null,
        supplierPN: row.supplier_pn ?? null,
        supplierTechnicalRevision: row.supplier_technical_revision ?? null,
        customerAccQty: row.customer_acc_qty != null ? Number(row.customer_acc_qty) : null,
        supplierAccQty: row.supplier_acc_qty != null ? Number(row.supplier_acc_qty) : null,
        transitAccQty: row.transit_acc_qty != null ? Number(row.transit_acc_qty) : null,
        customerLastInvoiceNumber: row.customer_last_invoice_number ?? null,
        supplierLastInvoiceNumber: row.supplier_last_invoice_number ?? null,
        transitInvoiceQty: row.transit_invoice_qty != null ? Number(row.transit_invoice_qty) : null,
        backlogFirmDate: toDateStr(row.backlog_firm_date),
        releaseFirmDate: toDateStr(row.release_firm_date),
        backlogFirmQty: row.backlog_firm_qty != null ? Number(row.backlog_firm_qty) : null,
        releaseFirmQty: row.release_firm_qty != null ? Number(row.release_firm_qty) : null,
        releasePreviousFirmQty: row.release_previous_firm_qty != null ? Number(row.release_previous_firm_qty) : null,
        firmQtyVariation: row.firm_qty_variation != null ? Number(row.firm_qty_variation) : null,
        variationType: row.variation_type ?? null,
        isMissing: Boolean(row.is_missing),
        isUncorrelated: Boolean(row.is_uncorrelated),
        hasQtyLessThanMinOrderQty: Boolean(row.has_qty_less_than_min_order_qty),
        hasQtyNotMultipleOfMinOrderQty: Boolean(row.has_qty_not_multiple_of_min_order_qty),
        backlogTotalQty: row.backlog_total_qty != null ? Number(row.backlog_total_qty) : null,
        releaseTotalQty: row.release_total_qty != null ? Number(row.release_total_qty) : null,
        totalQtyVariation: row.total_qty_variation != null ? Number(row.total_qty_variation) : null,
        analysisResultFirmDate: toDateStr(row.analysis_result_firm_date),
        analysisResultFirmQty: row.analysis_result_firm_qty != null ? Number(row.analysis_result_firm_qty) : null,
        analysisResultTotalQty: row.analysis_result_total_qty != null ? Number(row.analysis_result_total_qty) : null,
        comments: parseJsonArray(row.comments),
        isImplemented: Boolean(row.is_implemented),
        implementationComments: parseJsonArray(row.implementation_comments),
        deliveries: deliveries.map((d) => ({
          deliveryAnalysisId: String(d.id),
          sequence: d.sequence != null ? Number(d.sequence) : null,
          dueDate: toDateStr(d.due_date),
          deliveryTime: toTimeStr(d.delivery_time),
          backlogDeliveryType: d.backlog_delivery_type ?? null,
          backlogQty: d.backlog_qty != null ? Number(d.backlog_qty) : null,
          backlogAccQty: d.backlog_acc_qty != null ? Number(d.backlog_acc_qty) : null,
          releaseDeliveryType: d.release_delivery_type ?? null,
          releaseQty: d.release_qty != null ? Number(d.release_qty) : null,
          releaseAccQty: d.release_acc_qty != null ? Number(d.release_acc_qty) : null,
          qtyVariation: d.qty_variation != null ? Number(d.qty_variation) : null,
          accQtyVariation: d.acc_qty_variation != null ? Number(d.acc_qty_variation) : null,
          analysisResultDeliveryType: d.analysis_result_delivery_type ?? null,
          analysisResultQty: d.analysis_result_qty != null ? Number(d.analysis_result_qty) : null,
          analysisResultAccQty: d.analysis_result_acc_qty != null ? Number(d.analysis_result_acc_qty) : null,
          analysisResultQtyVariation: d.analysis_result_qty_variation != null ? Number(d.analysis_result_qty_variation) : null,
          analysisResultAccQtyVariation: d.analysis_result_acc_qty_variation != null ? Number(d.analysis_result_acc_qty_variation) : null,
          comments: parseJsonArray(d.comments),
        })),
      };
    });

    const totalPages = Math.max(1, Math.ceil(totalRecords / pageSize));
    const searchParams = {};
    if (filterByVariationTypeRaw) searchParams.filterByVariationType = filterByVariationTypeRaw;
    if (problemRaw) searchParams.problem = problemRaw;
    if (moqIssueRaw) searchParams.moqIssue = moqIssueRaw;
    if (supplierPNFilter) searchParams.supplierPN = supplierPNFilter;
    if (customerPNFilter) searchParams.customerPN = customerPNFilter;
    if (customerPurchaseOrderFilter) searchParams.customerPurchaseOrder = customerPurchaseOrderFilter;
    if (isImplementedRaw === "true" || isImplementedRaw === "false") searchParams.isImplemented = isImplementedRaw === "true";

    return res.status(200).json({
      success: true,
      data: {
        page,
        pageSize,
        sort: sortParam,
        searchParams,
        totalRecords,
        totalPages,
        records,
      },
    });
  } catch (err) {
    const msg = err && (err.message || err.code || String(err));
    console.error("GET /analysis/:releaseAnalysisId/items-with-deliveries:", msg);
    if (err && err.stack) console.error(err.stack);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: msg || "Erro desconhecido",
    });
  }
});

/**
 * GET /analysis/:releaseAnalysisId/report-data
 * Retorna dados completos do relatório da análise (ReportData): release, customer, totals, configs e itens com entregas.
 * 200: SuccessResponse < ReportData >
 * 400: releaseAnalysisId inválido. 404: Análise não encontrada (NullResponse). 500: Erro no servidor.
 */
router.get("/:releaseAnalysisId/report-data", async (req, res) => {
  try {
    const releaseAnalysisIdRaw = (req.params.releaseAnalysisId ?? "").toString().trim();
    if (!releaseAnalysisIdRaw) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "releaseAnalysisId é obrigatório.",
      });
    }
    const releaseAnalysisIdNum = parseInt(releaseAnalysisIdRaw, 10);
    if (Number.isNaN(releaseAnalysisIdNum) || releaseAnalysisIdNum < 1 || String(releaseAnalysisIdNum) !== releaseAnalysisIdRaw) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "releaseAnalysisId deve ser um identificador válido da análise.",
      });
    }

    const headerRows = await executarQueryMySQL(
      CLIENT_PREFIX,
      `SELECT ra.id, ra.analysis_version, ra.analysis_duration, ra.totals_json, ra.analysis_configs_json,
              ra.firm_policy, ra.custom_firm_days, ra.accept_increment, ra.accept_cut, ra.accept_date_variation,
              ra.transit_qty_policy, ra.create_order_if_not_exists, ra.auto_implement_analysis_result,
              ra.use_leadtime, ra.default_leadtime, ra.use_receipt, ra.updated_at AS analysis_updated_at,
              r.file_name, r.release_date, r.arrival_timestamp, r.customer_release_id,
              c.cnpj, c.internal_code, c.company_name, c.trade_name, c.alias, c.municipality, c.state, c.country
       FROM release_analyses ra
       JOIN releases r ON r.id = ra.release_id
       JOIN customers c ON c.id = r.customer_id
       WHERE ra.id = ?
       LIMIT 1`,
      [releaseAnalysisIdNum]
    );

    const header = Array.isArray(headerRows) && headerRows.length > 0 ? headerRows[0] : null;
    if (!header) {
      return res.status(404).json({
        success: true,
        data: null,
      });
    }

    const toDateStr = (v) => (v instanceof Date ? v.toISOString().slice(0, 10) : v != null ? String(v).slice(0, 10) : null);
    const toIso = (v) => (v instanceof Date ? v.toISOString() : v != null ? String(v) : null);
    const toTimeStr = (v) => (v == null ? null : typeof v === "string" ? v : v instanceof Date ? v.toTimeString().slice(0, 8) : String(v));
    const parseJsonArray = (v) => {
      if (v == null) return null;
      if (Array.isArray(v)) return v;
      if (typeof v === "string") {
        try {
          const p = JSON.parse(v);
          return Array.isArray(p) ? p : null;
        } catch (_) { return null; }
      }
      return null;
    };

    const defaultTotals = {
      analyzedItems: 0,
      validItems: 0,
      missingItems: 0,
      uncorrelatedItems: 0,
      itemsWithIncrementOnFirmPeriod: 0,
      itemsWithCutOnFirmPeriod: 0,
      itemsWithDateVariationOnFirmPeriod: 0,
      itemsWithoutVariationOnFirmPeriod: 0,
    };
    let totals = defaultTotals;
    if (header.totals_json) {
      try {
        const parsed = typeof header.totals_json === "string" ? JSON.parse(header.totals_json) : header.totals_json;
        if (parsed && typeof parsed === "object") totals = { ...defaultTotals, ...parsed };
      } catch (_) {}
    }
    let analysisConfigs = {
      firmPolicy: header.firm_policy ?? null,
      customFirmDays: header.custom_firm_days != null ? Number(header.custom_firm_days) : null,
      acceptIncrement: header.accept_increment != null ? Boolean(header.accept_increment) : null,
      acceptCut: header.accept_cut != null ? Boolean(header.accept_cut) : null,
      acceptDateVariation: header.accept_date_variation != null ? Boolean(header.accept_date_variation) : null,
      transitQtyPolicy: header.transit_qty_policy ?? null,
      createOrderIfNotExists: header.create_order_if_not_exists != null ? Boolean(header.create_order_if_not_exists) : null,
      autoImplementAnalysisResult: header.auto_implement_analysis_result != null ? Boolean(header.auto_implement_analysis_result) : null,
      useLeadtime: header.use_leadtime != null ? Boolean(header.use_leadtime) : null,
      defaultLeadtime: header.default_leadtime != null ? Number(header.default_leadtime) : null,
      useReceipt: header.use_receipt != null ? Boolean(header.use_receipt) : null,
    };
    if (header.analysis_configs_json) {
      try {
        const parsed = typeof header.analysis_configs_json === "string" ? JSON.parse(header.analysis_configs_json) : header.analysis_configs_json;
        if (parsed && typeof parsed === "object") {
          analysisConfigs = {
            firmPolicy: parsed.firmPolicy ?? parsed.firm_policy ?? analysisConfigs.firmPolicy,
            customFirmDays: parsed.customFirmDays ?? parsed.custom_firm_days ?? analysisConfigs.customFirmDays,
            acceptIncrement: parsed.acceptIncrement ?? parsed.accept_increment ?? analysisConfigs.acceptIncrement,
            acceptCut: parsed.acceptCut ?? parsed.accept_cut ?? analysisConfigs.acceptCut,
            acceptDateVariation: parsed.acceptDateVariation ?? parsed.accept_date_variation ?? analysisConfigs.acceptDateVariation,
            transitQtyPolicy: parsed.transitQtyPolicy ?? parsed.transit_qty_policy ?? analysisConfigs.transitQtyPolicy,
            createOrderIfNotExists: parsed.createOrderIfNotExists ?? parsed.create_order_if_not_exists ?? analysisConfigs.createOrderIfNotExists,
            autoImplementAnalysisResult: parsed.autoImplementAnalysisResult ?? parsed.auto_implement_analysis_result ?? analysisConfigs.autoImplementAnalysisResult,
            useLeadtime: parsed.useLeadtime ?? parsed.use_leadtime ?? analysisConfigs.useLeadtime,
            defaultLeadtime: parsed.defaultLeadtime ?? parsed.default_leadtime ?? analysisConfigs.defaultLeadtime,
            useReceipt: parsed.useReceipt ?? parsed.use_receipt ?? analysisConfigs.useReceipt,
          };
        }
      } catch (_) {}
    }

    const itemRows = await executarQueryMySQL(
      CLIENT_PREFIX,
      `SELECT id, sequence, customer_purchase_order, customer_pn, customer_technical_revision,
              supplier_pn, supplier_technical_revision, customer_acc_qty, supplier_acc_qty, transit_acc_qty,
              customer_last_invoice_number, supplier_last_invoice_number, transit_invoice_qty,
              backlog_firm_date, release_firm_date, backlog_firm_qty, release_firm_qty, release_previous_firm_qty,
              firm_qty_variation, variation_type, is_missing, is_uncorrelated,
              has_qty_less_than_min_order_qty, has_qty_not_multiple_of_min_order_qty,
              backlog_total_qty, release_total_qty, total_qty_variation,
              analysis_result_firm_date, analysis_result_firm_qty, analysis_result_total_qty,
              comments, is_implemented, implementation_comments
       FROM release_analysis_items
       WHERE release_analysis_id = ?
       ORDER BY sequence ASC`,
      [releaseAnalysisIdNum]
    );

    const itemList = Array.isArray(itemRows) ? itemRows : [];
    const itemIds = itemList.map((r) => r.id).filter((id) => id != null);
    let deliveriesByItem = {};
    if (itemIds.length > 0) {
      const placeholders = itemIds.map(() => "?").join(",");
      const delRows = await executarQueryMySQL(
        CLIENT_PREFIX,
        `SELECT analysis_item_id, id, sequence, due_date, delivery_time,
                backlog_delivery_type, backlog_qty, backlog_acc_qty, release_delivery_type, release_qty, release_acc_qty,
                qty_variation, acc_qty_variation, analysis_result_delivery_type, analysis_result_qty, analysis_result_acc_qty,
                analysis_result_qty_variation, analysis_result_acc_qty_variation, comments
         FROM release_analysis_deliveries
         WHERE analysis_item_id IN (${placeholders})
         ORDER BY analysis_item_id, sequence`,
        itemIds
      );
      const delList = Array.isArray(delRows) ? delRows : [];
      for (const d of delList) {
        const key = String(d.analysis_item_id);
        if (!deliveriesByItem[key]) deliveriesByItem[key] = [];
        deliveriesByItem[key].push(d);
      }
    }

    const itemsAnalysis = itemList.map((row) => {
      const deliveries = deliveriesByItem[String(row.id)] || [];
      return {
        itemAnalysisId: String(row.id),
        sequence: row.sequence != null ? Number(row.sequence) : null,
        customerPurchaseOrder: row.customer_purchase_order ?? null,
        customerPN: row.customer_pn ?? null,
        customerTechnicalRevision: row.customer_technical_revision ?? null,
        supplierPN: row.supplier_pn ?? null,
        supplierTechnicalRevision: row.supplier_technical_revision ?? null,
        customerAccQty: row.customer_acc_qty != null ? Number(row.customer_acc_qty) : null,
        supplierAccQty: row.supplier_acc_qty != null ? Number(row.supplier_acc_qty) : null,
        transitAccQty: row.transit_acc_qty != null ? Number(row.transit_acc_qty) : null,
        customerLastInvoiceNumber: row.customer_last_invoice_number ?? null,
        supplierLastInvoiceNumber: row.supplier_last_invoice_number ?? null,
        transitInvoiceQty: row.transit_invoice_qty != null ? Number(row.transit_invoice_qty) : null,
        backlogFirmDate: toDateStr(row.backlog_firm_date),
        releaseFirmDate: toDateStr(row.release_firm_date),
        backlogFirmQty: row.backlog_firm_qty != null ? Number(row.backlog_firm_qty) : null,
        releaseFirmQty: row.release_firm_qty != null ? Number(row.release_firm_qty) : null,
        releasePreviousFirmQty: row.release_previous_firm_qty != null ? Number(row.release_previous_firm_qty) : null,
        firmQtyVariation: row.firm_qty_variation != null ? Number(row.firm_qty_variation) : null,
        variationType: row.variation_type ?? null,
        isMissing: Boolean(row.is_missing),
        isUncorrelated: Boolean(row.is_uncorrelated),
        hasQtyLessThanMinOrderQty: Boolean(row.has_qty_less_than_min_order_qty),
        hasQtyNotMultipleOfMinOrderQty: Boolean(row.has_qty_not_multiple_of_min_order_qty),
        backlogTotalQty: row.backlog_total_qty != null ? Number(row.backlog_total_qty) : null,
        releaseTotalQty: row.release_total_qty != null ? Number(row.release_total_qty) : null,
        totalQtyVariation: row.total_qty_variation != null ? Number(row.total_qty_variation) : null,
        analysisResultFirmDate: toDateStr(row.analysis_result_firm_date),
        analysisResultFirmQty: row.analysis_result_firm_qty != null ? Number(row.analysis_result_firm_qty) : null,
        analysisResultTotalQty: row.analysis_result_total_qty != null ? Number(row.analysis_result_total_qty) : null,
        comments: parseJsonArray(row.comments),
        isImplemented: Boolean(row.is_implemented),
        implementationComments: parseJsonArray(row.implementation_comments),
        deliveries: deliveries.map((d) => ({
          deliveryAnalysisId: String(d.id),
          sequence: d.sequence != null ? Number(d.sequence) : null,
          dueDate: toDateStr(d.due_date),
          deliveryTime: toTimeStr(d.delivery_time),
          backlogDeliveryType: d.backlog_delivery_type ?? null,
          backlogQty: d.backlog_qty != null ? Number(d.backlog_qty) : null,
          backlogAccQty: d.backlog_acc_qty != null ? Number(d.backlog_acc_qty) : null,
          releaseDeliveryType: d.release_delivery_type ?? null,
          releaseQty: d.release_qty != null ? Number(d.release_qty) : null,
          releaseAccQty: d.release_acc_qty != null ? Number(d.release_acc_qty) : null,
          qtyVariation: d.qty_variation != null ? Number(d.qty_variation) : null,
          accQtyVariation: d.acc_qty_variation != null ? Number(d.acc_qty_variation) : null,
          analysisResultDeliveryType: d.analysis_result_delivery_type ?? null,
          analysisResultQty: d.analysis_result_qty != null ? Number(d.analysis_result_qty) : null,
          analysisResultAccQty: d.analysis_result_acc_qty != null ? Number(d.analysis_result_acc_qty) : null,
          analysisResultQtyVariation: d.analysis_result_qty_variation != null ? Number(d.analysis_result_qty_variation) : null,
          analysisResultAccQtyVariation: d.analysis_result_acc_qty_variation != null ? Number(d.analysis_result_acc_qty_variation) : null,
          comments: parseJsonArray(d.comments),
        })),
      };
    });

    const data = {
      releaseFileName: header.file_name ?? "",
      analysisVersion: Number(header.analysis_version) || 1,
      customer: {
        cnpj: header.cnpj ?? "",
        internalCode: header.internal_code ?? "",
        companyName: header.company_name ?? "",
        tradeName: header.trade_name ?? "",
        alias: header.alias ?? "",
        municipality: header.municipality ?? "",
        state: header.state ?? "",
        country: header.country ?? "",
      },
      releaseDate: toDateStr(header.release_date),
      arrivalTimestamp: toIso(header.arrival_timestamp),
      customerReleaseId: header.customer_release_id ?? "",
      analysisDuration: header.analysis_duration ?? "",
      analysisTimestamp: toIso(header.analysis_updated_at),
      totals: {
        analyzedItems: Number(totals.analyzedItems) || 0,
        validItems: Number(totals.validItems) || 0,
        missingItems: Number(totals.missingItems) || 0,
        uncorrelatedItems: Number(totals.uncorrelatedItems) || 0,
        itemsWithIncrementOnFirmPeriod: Number(totals.itemsWithIncrementOnFirmPeriod) || 0,
        itemsWithCutOnFirmPeriod: Number(totals.itemsWithCutOnFirmPeriod) || 0,
        itemsWithDateVariationOnFirmPeriod: Number(totals.itemsWithDateVariationOnFirmPeriod) || 0,
        itemsWithoutVariationOnFirmPeriod: Number(totals.itemsWithoutVariationOnFirmPeriod) || 0,
      },
      analysisConfigs,
      itemsAnalysis,
    };

    return res.status(200).json({
      success: true,
      data,
    });
  } catch (err) {
    const msg = err && (err.message || err.code || String(err));
    console.error("GET /analysis/:releaseAnalysisId/report-data:", msg);
    if (err && err.stack) console.error(err.stack);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: msg || "Erro desconhecido",
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
