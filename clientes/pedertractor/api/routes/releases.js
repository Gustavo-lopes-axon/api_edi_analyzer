const express = require("express");
const path = require("path");

const router = express.Router();

require("dotenv").config({ path: path.join(__dirname, "../../../../.env") });
const { getConnectionMySQL, executarQueryMySQL } = require("../../../../engines/mysqlClient.js");

const CLIENT_PREFIX = "PEDERTRACTOR";

const DATE_REGEX = /^\d{4}-\d{2}-\d{2}$/;
const CNPJ_DIGITS_REGEX = /^\d{14}$/;

/**
 * Valida o corpo do release (POST). Retorna { valid: true } ou { valid: false, message }.
 */
function validateReleaseBody(body) {
  const release = body?.release ?? body;
  if (!release || typeof release !== "object") {
    return { valid: false, message: "Corpo da requisição deve ser um objeto (dados do release)." };
  }

  const customer = release.customer;
  if (!customer || typeof customer !== "object") {
    return { valid: false, message: "Campo 'customer' é obrigatório." };
  }

  const internalCode = customer.internalCode ?? customer.internal_code;
  if (internalCode == null || String(internalCode).trim() === "") {
    return { valid: false, message: "customer.internalCode é obrigatório." };
  }

  const cnpj = (customer.cnpj ?? "").toString().replace(/\D/g, "");
  if (cnpj.length !== 14) {
    return { valid: false, message: "customer.cnpj é obrigatório (14 dígitos)." };
  }

  const customerReleaseId = release.customerReleaseId ?? release.customer_release_id;
  if (customerReleaseId == null || String(customerReleaseId).trim() === "") {
    return { valid: false, message: "customerReleaseId é obrigatório." };
  }

  const releaseDate = release.releaseDate ?? release.release_date;
  if (!releaseDate || typeof releaseDate !== "string" || !releaseDate.trim()) {
    return { valid: false, message: "releaseDate é obrigatório." };
  }
  if (!DATE_REGEX.test(releaseDate.trim())) {
    return { valid: false, message: "releaseDate deve estar no formato YYYY-MM-DD." };
  }

  const items = release.items;
  if (items !== undefined && !Array.isArray(items)) {
    return { valid: false, message: "items deve ser um array (pode ser vazio)." };
  }

  return { valid: true, release };
}


/**
 * GET /releases/status
 * Parâmetros obrigatórios: customerCnpj (14 dígitos), customerReleaseId, releaseDate (YYYY-MM-DD).
 * Retorna ReleaseStatusResponse; se o release não for encontrado/carregado: releaseId null, releaseStatus "not_loaded", analysisStatus "not_analyzed", timesAnalyzed 0.
 */
router.get("/status", async (req, res) => {
  try {
    const customerCnpj = (req.query.customerCnpj ?? "").toString().replace(/\D/g, "").trim();
    const customerReleaseId = req.query.customerReleaseId?.trim();
    const releaseDate = req.query.releaseDate?.trim();

    const missing = [];
    if (!customerCnpj) missing.push("customerCnpj");
    if (!customerReleaseId) missing.push("customerReleaseId");
    if (!releaseDate) missing.push("releaseDate");

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
        message: "customerCnpj deve conter exatamente 14 dígitos (somente números)",
      });
    }

    if (!DATE_REGEX.test(releaseDate)) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: "releaseDate deve estar no formato YYYY-MM-DD",
      });
    }

    let data = {
      customerCnpj,
      customerReleaseId,
      releaseDate,
      releaseId: null,
      releaseStatus: "not_loaded",
      analysisStatus: "not_analyzed",
      timesAnalyzed: 0,
    };

    const rows = await executarQueryMySQL(
      CLIENT_PREFIX,
      `SELECT
         r.customer_release_id,
         r.release_date,
         r.release_status,
         r.id AS release_id
       FROM releases r
       JOIN customers c ON c.id = r.customer_id
       WHERE c.cnpj = ?
         AND r.customer_release_id = ?
         AND r.release_date = ?
       LIMIT 1`,
      [customerCnpj, customerReleaseId, releaseDate]
    );

    if (rows && rows.length > 0) {
      const row = rows[0];
      data = {
        customerCnpj,
        customerReleaseId,
        releaseDate,
        releaseId: String(row.release_id),
        releaseStatus: row.release_status ?? "not_loaded",
        analysisStatus: "not_analyzed",
        timesAnalyzed: 0,
      };
    }

    return res.status(200).json({
      success: true,
      data,
    });
  } catch (err) {
    console.error("GET /releases/status:", err.message);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: err.message,
    });
  }
});

/** Valores aceitos para releaseStatus (estado de carregamento do release) */
const RELEASE_STATUS_VALUES = ["loading", "loaded", "load_failed"];

/**
 * POST /releases/status
 * Atualiza o estado de carregamento de um release.
 * Corpo: { releaseId, timestamp, releaseStatus } (todos obrigatórios).
 * Retorna 200 com UpdateResponse { numberOfRecordsUpdated }.
 */
router.post("/status", async (req, res) => {
  try {
    const body = req.body ?? {};
    const releaseId = body.releaseId ?? body.release_id;
    const timestamp = body.timestamp;
    const releaseStatus = body.releaseStatus ?? body.release_status;

    const missing = [];
    if (releaseId == null || String(releaseId).trim() === "") missing.push("releaseId");
    if (timestamp == null || String(timestamp).trim() === "") missing.push("timestamp");
    if (releaseStatus == null || String(releaseStatus).trim() === "") missing.push("releaseStatus");

    if (missing.length > 0) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: `Campos obrigatórios: ${missing.join(", ")}`,
      });
    }

    const status = String(releaseStatus).trim().toLowerCase();
    if (!RELEASE_STATUS_VALUES.includes(status)) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: `releaseStatus deve ser um dos: ${RELEASE_STATUS_VALUES.join(", ")}`,
      });
    }

    const safeId = String(releaseId).trim();
    const conn = await getConnectionMySQL(CLIENT_PREFIX);
    let result;
    try {
      [result] = await conn.execute(
        "UPDATE releases SET release_status = ? WHERE customer_release_id = ? OR custom_id LIKE ?",
        [status, safeId, `%|r:${safeId}`]
      );
    } finally {
      conn.release();
    }

    const numberOfRecordsUpdated = result.affectedRows ?? 0;

    if (process.env.NODE_ENV === "development") {
      console.log(`[POST /releases/status] releaseId=${releaseId}, releaseStatus=${status}, updated=${numberOfRecordsUpdated}`);
    }

    return res.status(200).json({
      success: true,
      data: {
        numberOfRecordsUpdated,
      },
    });
  } catch (err) {
    console.error("POST /releases/status:", err.message);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: err.message,
    });
  }
});

/**
 * Converts a value to a MySQL-safe DATE string (YYYY-MM-DD) or null.
 * @param {*} value
 * @returns {string|null}
 */
function toDate(value) {
  if (value == null) return null;
  const s = String(value).trim();
  return DATE_REGEX.test(s) ? s : null;
}

/**
 * Converts a value to a MySQL-safe DATETIME string or null.
 * Accepts ISO-8601 strings (e.g. "2024-05-21T14:20:11.223Z").
 * @param {*} value
 * @returns {string|null}
 */
function toDatetime(value) {
  if (value == null) return null;
  const d = new Date(value);
  if (isNaN(d.getTime())) return null;
  return d.toISOString().replace("T", " ").replace("Z", "");
}

/**
 * Converts a value to a number or null.
 * @param {*} value
 * @returns {number|null}
 */
function toNum(value) {
  if (value == null) return null;
  const n = Number(value);
  return isNaN(n) ? null : n;
}

/**
 * POST /releases
 * Corpo: JSON com dados do release (ou { "release": { ... } }).
 * Persiste customer, release, items e deliveries no MySQL em uma única transação.
 * Retorna 201 com InsertionResponse { recordId }.
 */
router.post("/", async (req, res) => {
  let conn;
  try {
    const validation = validateReleaseBody(req.body);
    if (!validation.valid) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: validation.message,
      });
    }

    const release = validation.release;
    const customer = release.customer;
    const internalCode = String(customer.internalCode ?? customer.internal_code ?? "").trim();
    const customerReleaseId = String(release.customerReleaseId ?? release.customer_release_id ?? "").trim();
    const recordId = `${internalCode}${customerReleaseId}`;

    conn = await getConnectionMySQL(CLIENT_PREFIX);
    await conn.beginTransaction();

    // ------------------------------------------------------------------
    // 1. Upsert customer
    // ------------------------------------------------------------------
    const cnpj = String(customer.cnpj ?? "").replace(/\D/g, "");
    await conn.execute(
      `INSERT INTO customers
         (cnpj, internal_code, company_name, trade_name, alias, municipality, state, country)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         internal_code = VALUES(internal_code),
         company_name  = VALUES(company_name),
         trade_name    = VALUES(trade_name),
         alias         = VALUES(alias),
         municipality  = VALUES(municipality),
         state         = VALUES(state),
         country       = VALUES(country)`,
      [
        cnpj,
        internalCode,
        customer.companyName ?? customer.company_name ?? null,
        customer.tradeName ?? customer.trade_name ?? null,
        customer.alias ?? null,
        customer.municipality ?? null,
        customer.state ?? null,
        customer.country ?? null,
      ]
    );

    const [[customerRow]] = await conn.execute(
      "SELECT id FROM customers WHERE cnpj = ?",
      [cnpj]
    );
    const customerId = customerRow.id;

    // ------------------------------------------------------------------
    // 2. Upsert release header (status = 'loading')
    // ------------------------------------------------------------------
    const releaseCustomId = String(release.customId ?? release.custom_id ?? "").trim();
    await conn.execute(
      `INSERT INTO releases
         (custom_id, customer_id, customer_release_id, release_date, file_name,
          receipt_file_name, arrival_timestamp, items_qty, deliveries_qty, \`force\`, release_status)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'loading')
       ON DUPLICATE KEY UPDATE
         customer_id         = VALUES(customer_id),
         customer_release_id = VALUES(customer_release_id),
         release_date        = VALUES(release_date),
         file_name           = VALUES(file_name),
         receipt_file_name   = VALUES(receipt_file_name),
         arrival_timestamp   = VALUES(arrival_timestamp),
         items_qty           = VALUES(items_qty),
         deliveries_qty      = VALUES(deliveries_qty),
         \`force\`            = VALUES(\`force\`),
         release_status      = 'loading'`,
      [
        releaseCustomId,
        customerId,
        customerReleaseId,
        toDate(release.releaseDate ?? release.release_date),
        release.fileName ?? release.file_name ?? null,
        release.receiptFileName ?? release.receipt_file_name ?? null,
        toDatetime(release.arrivalTimestamp ?? release.arrival_timestamp),
        toNum(release.itemsQty ?? release.items_qty) ?? 0,
        toNum(release.deliveriesQty ?? release.deliveries_qty) ?? 0,
        release.force ? 1 : 0,
      ]
    );

    const [[releaseRow]] = await conn.execute(
      "SELECT id FROM releases WHERE custom_id = ?",
      [releaseCustomId]
    );
    const releaseId = releaseRow.id;

    // ------------------------------------------------------------------
    // 3. Upsert items and their deliveries
    // ------------------------------------------------------------------
    const items = Array.isArray(release.items) ? release.items : [];

    for (const item of items) {
      const itemCustomId = String(item.customId ?? item.custom_id ?? "").trim();

      await conn.execute(
        `INSERT INTO release_items
           (custom_id, release_id, sequence, customer_purchase_order, purchase_order_line,
            program_id, program_date, program_type, customer_pn, technical_revision,
            supplier_pn, unit_of_measure, min_batch_qty, last_received_date, last_received_qty,
            last_invoice_number, last_invoice_series, last_invoice_date, last_acc_qty,
            last_acc_needed_qty, acc_start_date, delivery_location, contact_person,
            supply_type, supply_frequency_code, production_authorization_date,
            raw_material_authorization_date, unload_location, item_status_code, notes)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
           release_id                      = VALUES(release_id),
           sequence                        = VALUES(sequence),
           customer_purchase_order         = VALUES(customer_purchase_order),
           purchase_order_line             = VALUES(purchase_order_line),
           program_id                      = VALUES(program_id),
           program_date                    = VALUES(program_date),
           program_type                    = VALUES(program_type),
           customer_pn                     = VALUES(customer_pn),
           technical_revision              = VALUES(technical_revision),
           supplier_pn                     = VALUES(supplier_pn),
           unit_of_measure                 = VALUES(unit_of_measure),
           min_batch_qty                   = VALUES(min_batch_qty),
           last_received_date              = VALUES(last_received_date),
           last_received_qty               = VALUES(last_received_qty),
           last_invoice_number             = VALUES(last_invoice_number),
           last_invoice_series             = VALUES(last_invoice_series),
           last_invoice_date               = VALUES(last_invoice_date),
           last_acc_qty                    = VALUES(last_acc_qty),
           last_acc_needed_qty             = VALUES(last_acc_needed_qty),
           acc_start_date                  = VALUES(acc_start_date),
           delivery_location               = VALUES(delivery_location),
           contact_person                  = VALUES(contact_person),
           supply_type                     = VALUES(supply_type),
           supply_frequency_code           = VALUES(supply_frequency_code),
           production_authorization_date   = VALUES(production_authorization_date),
           raw_material_authorization_date = VALUES(raw_material_authorization_date),
           unload_location                 = VALUES(unload_location),
           item_status_code                = VALUES(item_status_code),
           notes                           = VALUES(notes)`,
        [
          itemCustomId,
          releaseId,
          toNum(item.sequence),
          item.customerPurchaseOrder ?? item.customer_purchase_order ?? null,
          toNum(item.purchaseOrderLine ?? item.purchase_order_line),
          item.programId ?? item.program_id ?? null,
          toDate(item.programDate ?? item.program_date),
          item.programType ?? item.program_type ?? null,
          item.customerPN ?? item.customer_pn ?? null,
          item.technicalRevision ?? item.technical_revision ?? null,
          item.supplierPN ?? item.supplier_pn ?? null,
          item.unitOfMeasure ?? item.unit_of_measure ?? null,
          toNum(item.minBatchQty ?? item.min_batch_qty),
          toDate(item.lastReceivedDate ?? item.last_received_date),
          toNum(item.lastReceivedQty ?? item.last_received_qty),
          item.lastInvoiceNumber ?? item.last_invoice_number ?? null,
          item.lastInvoiceSeries ?? item.last_invoice_series ?? null,
          toDate(item.lastInvoiceDate ?? item.last_invoice_date),
          toNum(item.lastAccQty ?? item.last_acc_qty),
          toNum(item.lastAccNeededQty ?? item.last_acc_needed_qty),
          toDate(item.accStartDate ?? item.acc_start_date),
          item.deliveryLocation ?? item.delivery_location ?? null,
          item.contactPerson ?? item.contact_person ?? null,
          item.supplyType ?? item.supply_type ?? null,
          item.supplyFrequencyCode ?? item.supply_frequency_code ?? null,
          toDate(item.productionAuthorizationDate ?? item.production_authorization_date),
          toDate(item.rawMaterialAuthorizationDate ?? item.raw_material_authorization_date),
          item.unloadLocation ?? item.unload_location ?? null,
          item.itemStatusCode ?? item.item_status_code ?? null,
          item.notes ?? null,
        ]
      );

      const [[itemRow]] = await conn.execute(
        "SELECT id FROM release_items WHERE custom_id = ?",
        [itemCustomId]
      );
      const itemId = itemRow.id;

      const deliveries = Array.isArray(item.deliveries) ? item.deliveries : [];

      for (const delivery of deliveries) {
        const deliveryCustomId = String(delivery.customId ?? delivery.custom_id ?? "").trim();

        await conn.execute(
          `INSERT INTO release_deliveries
             (custom_id, item_id, sequence, type, due_date, delivery_time,
              qty, delivery_window_start, acc_qty)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON DUPLICATE KEY UPDATE
             item_id               = VALUES(item_id),
             sequence              = VALUES(sequence),
             type                  = VALUES(type),
             due_date              = VALUES(due_date),
             delivery_time         = VALUES(delivery_time),
             qty                   = VALUES(qty),
             delivery_window_start = VALUES(delivery_window_start),
             acc_qty               = VALUES(acc_qty)`,
          [
            deliveryCustomId,
            itemId,
            toNum(delivery.sequence),
            delivery.type ?? null,
            toDate(delivery.dueDate ?? delivery.due_date),
            delivery.deliveryTime ?? delivery.delivery_time ?? null,
            toNum(delivery.qty),
            toDatetime(delivery.deliveryWindowStart ?? delivery.delivery_window_start),
            toNum(delivery.accQty ?? delivery.acc_qty),
          ]
        );
      }
    }

    // ------------------------------------------------------------------
    // 4. Mark release as loaded
    // ------------------------------------------------------------------
    await conn.execute(
      "UPDATE releases SET release_status = 'loaded' WHERE id = ?",
      [releaseId]
    );

    await conn.commit();

    if (process.env.NODE_ENV === "development") {
      console.log(`[POST /releases] recordId=${recordId}, releaseId=${releaseId}, items=${items.length}`);
    }

    return res.status(201).json({
      success: true,
      data: {
        recordId,
      },
    });
  } catch (err) {
    if (conn) {
      try {
        await conn.rollback();
      } catch (_) {}
    }
    console.error("POST /releases:", err.message);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: err.message,
    });
  } finally {
    if (conn) conn.release();
  }
});

module.exports = router;
