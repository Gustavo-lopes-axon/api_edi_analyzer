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
 * GET /releases
 * Lista releases com paginação e filtros.
 * Query: page, pageSize, sort (-releaseDate|+releaseDate|customer|customerReleaseId), customer, customerReleaseId, startDate, endDate, status.
 * Retorna { success, data: { page, pageSize, sort, searchParams, totalRecords, totalPages, records } }.
 */
router.get("/", (req, res) => {
  const run = async () => {
    try {
      console.log("[GET /releases] handler start");
      const query = req.query || {};
      const page = Math.max(1, parseInt(query.page, 10) || 1);
      const pageSize = Math.min(100, Math.max(1, parseInt(query.pageSize, 10) || 30));
      const sortParam = String(query.sort ?? "-releaseDate").trim() || "-releaseDate";
      const customerFilter = (query.customer ?? "").toString().trim();
      const customerReleaseIdFilter = (query.customerReleaseId ?? "").toString().trim();
      const startDate = (query.startDate ?? "").toString().trim();
      const endDate = (query.endDate ?? "").toString().trim();
      const statusFilter = (query.status ?? "").toString().trim();

      const isDesc = sortParam.startsWith("-");
      const sortFieldRaw = (isDesc ? sortParam.slice(1) : sortParam.replace(/^\+/, "")).trim() || "release_date";
      const sortMap = {
        releaseDate: "r.release_date",
        customer: "c.company_name",
        customerReleaseId: "r.customer_release_id",
      };
      const orderBy = sortMap[sortFieldRaw] || "r.release_date";
      const orderDir = isDesc ? "DESC" : "ASC";
      const offset = Math.max(0, (page - 1) * pageSize);

      const conditions = [];
      const countParams = [];
      if (customerFilter) {
        const escaped = customerFilter.replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_");
        const likeVal = `%${escaped}%`;
        conditions.push("(c.company_name LIKE ? OR c.trade_name LIKE ? OR c.alias LIKE ? OR c.cnpj LIKE ?)");
        countParams.push(likeVal, likeVal, likeVal, likeVal);
      }
      if (customerReleaseIdFilter) {
        conditions.push("r.customer_release_id = ?");
        countParams.push(customerReleaseIdFilter);
      }
      if (startDate && DATE_REGEX.test(startDate)) {
        conditions.push("r.release_date >= ?");
        countParams.push(startDate);
      }
      if (endDate && DATE_REGEX.test(endDate)) {
        conditions.push("r.release_date <= ?");
        countParams.push(endDate);
      }
      if (statusFilter) {
        conditions.push("r.release_status = ?");
        countParams.push(statusFilter);
      }
      const whereClause = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

      const countRows = await executarQueryMySQL(
        CLIENT_PREFIX,
        `SELECT COUNT(*) AS total FROM releases r JOIN customers c ON c.id = r.customer_id ${whereClause}`,
        countParams
      );
      console.log("[GET /releases] after count");
      const total = Number(countRows?.[0]?.total ?? 0);

      const limitNum = Math.min(100, Math.max(0, Math.floor(Number(pageSize)) || 30));
      const offsetNum = Math.max(0, Math.floor(Number(offset)) || 0);
      const rows = await executarQueryMySQL(
        CLIENT_PREFIX,
        `SELECT r.id, r.custom_id, r.customer_release_id, r.release_date, r.release_status,
                r.file_name, r.receipt_file_name, r.arrival_timestamp, r.items_qty, r.deliveries_qty,
                c.cnpj AS customer_cnpj, c.internal_code AS customer_internal_code, c.company_name AS customer_company_name,
                c.trade_name AS customer_trade_name, c.alias AS customer_alias,
                c.municipality AS customer_municipality, c.state AS customer_state, c.country AS customer_country
         FROM releases r
         JOIN customers c ON c.id = r.customer_id
         ${whereClause}
         ORDER BY ${orderBy} ${orderDir}
         LIMIT ${limitNum} OFFSET ${offsetNum}`,
        countParams
      );
      console.log("[GET /releases] after list query");

      const releaseList = Array.isArray(rows) ? rows : [];
      const toDateStr = (v) => (v instanceof Date ? v.toISOString().slice(0, 10) : v != null ? String(v).slice(0, 10) : "");
      const toIso = (v) => (v instanceof Date ? v.toISOString() : v != null ? String(v) : null);
      const releases = releaseList.map((row) => {
        const pk = row.id ?? row.ID ?? row.Id;
        const idValStr = pk != null ? String(pk) : "";
        return {
          releaseId: idValStr,
          customer: {
            cnpj: row.customer_cnpj ?? "",
            internalCode: row.customer_internal_code ?? "",
            companyName: row.customer_company_name ?? "",
            tradeName: row.customer_trade_name ?? "",
            alias: row.customer_alias ?? "",
            municipality: row.customer_municipality ?? "",
            state: row.customer_state ?? "",
            country: row.customer_country ?? "",
          },
          customerReleaseId: row.customer_release_id ?? "",
          releaseDate: toDateStr(row.release_date),
          fileName: row.file_name ?? "",
          receiptFileName: row.receipt_file_name ?? "",
          arrivalTimestamp: toIso(row.arrival_timestamp),
          itemsQty: Number(row.items_qty) || 0,
          deliveriesQty: Number(row.deliveries_qty) || 0,
          releaseStatus: row.release_status ?? "",
        };
      });

      const totalRecords = total;
      const totalPages = Math.max(1, Math.ceil(totalRecords / pageSize));

      const searchParams = {};
      if (customerFilter) searchParams.customer = customerFilter;
      if (startDate) searchParams.startDate = startDate;
      if (endDate) searchParams.endDate = endDate;
      if (customerReleaseIdFilter) searchParams.customerReleaseId = customerReleaseIdFilter;
      if (statusFilter) searchParams.status = statusFilter;

      console.log("[GET /releases] before send, records:", releases.length);
      const payload = {
        success: true,
        data: {
          page,
          pageSize,
          sort: sortParam,
          searchParams,
          totalRecords,
          totalPages,
          records: releases,
        },
      };
    let jsonStr;
    try {
      jsonStr = JSON.stringify(payload);
    } catch (serializeErr) {
      console.error("[GET /releases] JSON.stringify error:", serializeErr && (serializeErr.message || String(serializeErr)));
      if (!res.headersSent) {
        return res.status(500).json({
          success: false,
          error: "Internal server error",
          message: "Response serialization failed",
        });
      }
      return;
    }
    res.status(200).setHeader("Content-Type", "application/json; charset=utf-8").send(jsonStr);
    console.log("[GET /releases] response sent");
    } catch (err) {
      const msg = err && (err.message || err.code || String(err));
      console.error("GET /releases:", msg);
      if (err && err.stack) console.error(err.stack);
      if (!res.headersSent) {
        return res.status(500).json({
          success: false,
          error: "Internal server error",
          message: msg || "Erro desconhecido",
        });
      }
    }
  };
  run().catch((err) => {
    console.error("GET /releases unhandled rejection:", err && (err.message || err.code || String(err)));
    if (err && err.stack) console.error(err.stack);
    if (!res.headersSent) {
      res.status(500).json({
        success: false,
        error: "Internal server error",
        message: (err && (err.message || err.code || String(err))) || "Erro desconhecido",
      });
    }
  });
});

/**
 * GET /releases/all-items
 * Lista informações dos itens (ReleaseItemInfo) com paginação e filtros.
 * Query: page (default 1), pageSize (default 30), sort (default "+supplierPN,-releaseDate"), supplierPN, customerPN, description.
 * Ordenação: +campo asc, -campo desc; campos: supplierPN, customerPN, releaseDate, createdAt, customerPurchaseOrder.
 * Retorna SuccessResponse < PaginatedResponse < ReleaseItemInfo, ReleaseItemInfoFilterParams >>.
 */
router.get("/all-items", async (req, res) => {
  try {
    const query = req.query || {};
    const page = Math.max(1, parseInt(query.page, 10) || 1);
    const pageSize = Math.min(100, Math.max(1, parseInt(query.pageSize, 10) || 30));
    const sortParam = String(query.sort ?? "+supplierPN,-releaseDate").trim() || "+supplierPN,-releaseDate";
    const supplierPNFilter = (query.supplierPN ?? "").toString().trim();
    const customerPNFilter = (query.customerPN ?? "").toString().trim();
    const descriptionFilter = (query.description ?? "").toString().trim();

    const conditions = [];
    const countParams = [];
    if (supplierPNFilter) {
      conditions.push("ri.supplier_pn = ?");
      countParams.push(supplierPNFilter);
    }
    if (customerPNFilter) {
      conditions.push("ri.customer_pn = ?");
      countParams.push(customerPNFilter);
    }
    if (descriptionFilter) {
      const escaped = descriptionFilter.replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_");
      conditions.push("ri.notes LIKE ?");
      countParams.push(`%${escaped}%`);
    }
    const whereClause = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

    const sortMap = {
      supplierPN: "ri.supplier_pn",
      customerPN: "ri.customer_pn",
      releaseDate: "r.release_date",
      createdAt: "ri.created_at",
      customerPurchaseOrder: "ri.customer_purchase_order",
    };
    const orderParts = sortParam.split(",").map((s) => s.trim()).filter(Boolean);
    const orderClauses = orderParts.length
      ? orderParts.map((part) => {
          const isDesc = part.startsWith("-");
          const field = (isDesc ? part.slice(1) : part.replace(/^\+/, "")).trim() || "supplierPN";
          const col = sortMap[field] || "ri.supplier_pn";
          return `${col} ${isDesc ? "DESC" : "ASC"}`;
        })
      : ["ri.supplier_pn ASC", "r.release_date DESC"];
    const orderBy = orderClauses.join(", ");
    const offset = Math.max(0, (page - 1) * pageSize);
    const limitNum = Math.min(100, pageSize);
    const offsetNum = Math.max(0, offset);

    const countRows = await executarQueryMySQL(
      CLIENT_PREFIX,
      `SELECT COUNT(*) AS total FROM release_items ri JOIN releases r ON r.id = ri.release_id ${whereClause}`,
      countParams
    );
    const totalRecords = Number(countRows?.[0]?.total ?? 0);

    const rows = await executarQueryMySQL(
      CLIENT_PREFIX,
      `SELECT ri.id, ri.customer_pn, ri.technical_revision, ri.supplier_pn, ri.notes,
              r.customer_release_id, r.release_date,
              c.trade_name AS customer_trade_name, c.alias AS customer_alias, c.company_name AS customer_company_name
       FROM release_items ri
       JOIN releases r ON r.id = ri.release_id
       JOIN customers c ON c.id = r.customer_id
       ${whereClause}
       ORDER BY ${orderBy}
       LIMIT ${limitNum} OFFSET ${offsetNum}`,
      countParams
    );

    const toDateStr = (v) => (v instanceof Date ? v.toISOString().slice(0, 10) : v != null ? String(v).slice(0, 10) : "");
    const itemList = Array.isArray(rows) ? rows : [];
    const records = itemList.map((row) => {
      const customerName = row.customer_trade_name ?? row.customer_alias ?? row.customer_company_name ?? "";
      return {
        customerPN: row.customer_pn ?? "",
        customerTechnicalRevision: row.technical_revision ?? null,
        supplierPN: row.supplier_pn ?? "",
        supplierTechnicalRevision: null,
        lifecycleStage: null,
        category: null,
        leadTime: null,
        description: row.notes ?? "",
        lastRelease: {
          customerReleaseId: row.customer_release_id ?? "",
          date: toDateStr(row.release_date),
          customerName,
        },
      };
    });

    const totalPages = Math.max(1, Math.ceil(totalRecords / pageSize));
    const searchParams = {};
    if (supplierPNFilter) searchParams.supplierPN = supplierPNFilter;
    if (customerPNFilter) searchParams.customerPN = customerPNFilter;
    if (descriptionFilter) searchParams.description = descriptionFilter;

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
    console.error("GET /releases/all-items:", msg);
    if (err && err.stack) console.error(err.stack);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: msg || "Erro desconhecido",
    });
  }
});

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

/**
 * GET /releases/timeline
 * Histórico de quantidades de um item por release (customerPN obrigatório; startDate/endDate opcionais, YYYY-MM-DD).
 * 200: SuccessResponse < ReleaseItemTimeline >
 * 400: Parâmetros inválidos (ErrorResponse)
 * 500: Erro no servidor (ErrorResponse)
 */
router.get("/timeline", async (req, res) => {
  try {
    const customerPN = (req.query.customerPN ?? "").toString().trim();
    const startDate = (req.query.startDate ?? "").toString().trim();
    const endDate = (req.query.endDate ?? "").toString().trim();

    if (!customerPN) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "customerPN é obrigatório.",
      });
    }
    if (startDate && !DATE_REGEX.test(startDate)) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "startDate deve estar no formato YYYY-MM-DD.",
      });
    }
    if (endDate && !DATE_REGEX.test(endDate)) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "endDate deve estar no formato YYYY-MM-DD.",
      });
    }

    const recordParams = [customerPN];
    let recordWhere = "ri.customer_pn = ?";
    if (startDate) {
      recordWhere += " AND r.release_date >= ?";
      recordParams.push(startDate);
    }
    if (endDate) {
      recordWhere += " AND r.release_date <= ?";
      recordParams.push(endDate);
    }

    const itemMeta = await executarQueryMySQL(
      CLIENT_PREFIX,
      `SELECT technical_revision, supplier_pn, notes FROM release_items WHERE customer_pn = ? ORDER BY id DESC LIMIT 1`,
      [customerPN]
    );
    const meta = Array.isArray(itemMeta) && itemMeta.length > 0 ? itemMeta[0] : null;
    const customerTechnicalRevision = meta?.technical_revision ?? null;
    const supplierPN = meta?.supplier_pn ?? "";
    const description = meta?.notes ?? "";

    const orderMeta = await executarQueryMySQL(
      CLIENT_PREFIX,
      `SELECT supplier_technical_revision, lifecycle_stage, category, lead_time
       FROM order_items WHERE customer_pn = ? ORDER BY id DESC LIMIT 1`,
      [customerPN]
    );
    const oi = Array.isArray(orderMeta) && orderMeta.length > 0 ? orderMeta[0] : null;
    const supplierTechnicalRevision = oi?.supplier_technical_revision ?? null;
    const lifecycleStage = oi?.lifecycle_stage ?? null;
    const category = oi?.category ?? null;
    const leadTime = oi?.lead_time != null ? Number(oi.lead_time) : null;

    const rows = await executarQueryMySQL(
      CLIENT_PREFIX,
      `SELECT r.release_date, r.customer_release_id,
              c.trade_name AS customer_trade_name, c.alias AS customer_alias, c.company_name AS customer_company_name,
              COALESCE(SUM(rd.qty), 0) AS total_qty,
              COALESCE(SUM(CASE WHEN LOWER(TRIM(rd.type)) = 'firm' THEN rd.qty ELSE 0 END), 0) AS firm_qty,
              COALESCE(SUM(CASE WHEN LOWER(TRIM(rd.type)) = 'planning' THEN rd.qty ELSE 0 END), 0) AS planning_qty,
              COALESCE(SUM(CASE WHEN rd.due_date IS NOT NULL AND rd.due_date < CURDATE() THEN rd.qty ELSE 0 END), 0) AS past_due_qty
       FROM release_items ri
       JOIN releases r ON r.id = ri.release_id
       JOIN customers c ON c.id = r.customer_id
       LEFT JOIN release_deliveries rd ON rd.item_id = ri.id
       WHERE ${recordWhere}
       GROUP BY r.id
       ORDER BY r.release_date, r.customer_release_id, c.id`,
      recordParams
    );

    const toDateStr = (v) => (v instanceof Date ? v.toISOString().slice(0, 10) : v != null ? String(v).slice(0, 10) : "");
    const list = Array.isArray(rows) ? rows : [];
    const records = list.map((row) => {
      const customerName = row.customer_trade_name ?? row.customer_alias ?? row.customer_company_name ?? "";
      return {
        releaseDate: toDateStr(row.release_date),
        customerReleaseId: row.customer_release_id ?? "",
        customerName,
        totalQty: Number(row.total_qty) || 0,
        firmQty: Number(row.firm_qty) || 0,
        planningQty: Number(row.planning_qty) || 0,
        pastDueQty: Number(row.past_due_qty) || 0,
      };
    });

    const data = {
      customerPN,
      customerTechnicalRevision,
      supplierPN,
      supplierTechnicalRevision,
      lifecycleStage,
      category,
      leadTime,
      startDate: startDate || null,
      endDate: endDate || null,
      description,
      records,
    };

    return res.status(200).json({
      success: true,
      data,
    });
  } catch (err) {
    const msg = err && (err.message || err.code || String(err));
    console.error("GET /releases/timeline:", msg);
    if (err && err.stack) console.error(err.stack);
    if (!res.headersSent) {
      return res.status(500).json({
        success: false,
        error: "Internal server error",
        message: msg || "Erro desconhecido",
      });
    }
  }
});

/**
 * GET /releases/:releaseId/header
 * Retorna o cabeçalho do release pelo id.
 * 200: SuccessResponse com ReleaseHeader
 * 400: releaseId ausente ou inválido (ErrorResponse)
 * 404: Release não encontrado (NullResponse: success true, data null)
 * 500: Erro no servidor (ErrorResponse)
 */
router.get("/:releaseId/header", async (req, res) => {
  try {
    const releaseIdRaw = (req.params.releaseId ?? "").toString().trim();
    if (!releaseIdRaw) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "releaseId é obrigatório.",
      });
    }
    const releaseIdNum = parseInt(releaseIdRaw, 10);
    if (Number.isNaN(releaseIdNum) || releaseIdNum < 1 || String(releaseIdNum) !== releaseIdRaw) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "releaseId deve ser um identificador válido do release.",
      });
    }

    const rows = await executarQueryMySQL(
      CLIENT_PREFIX,
      `SELECT r.id, r.customer_release_id, r.release_date, r.release_status,
              r.file_name, r.receipt_file_name, r.arrival_timestamp, r.items_qty, r.deliveries_qty,
              c.cnpj AS customer_cnpj, c.internal_code AS customer_internal_code, c.company_name AS customer_company_name,
              c.trade_name AS customer_trade_name, c.alias AS customer_alias,
              c.municipality AS customer_municipality, c.state AS customer_state, c.country AS customer_country
       FROM releases r
       JOIN customers c ON c.id = r.customer_id
       WHERE r.id = ?`,
      [releaseIdNum]
    );

    const row = Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
    if (!row) {
      return res.status(404).json({
        success: true,
        data: null,
      });
    }

    const toDateStr = (v) => (v instanceof Date ? v.toISOString().slice(0, 10) : v != null ? String(v).slice(0, 10) : "");
    const toIso = (v) => (v instanceof Date ? v.toISOString() : v != null ? String(v) : null);
    const pk = row.id ?? row.ID ?? row.Id;
    const idValStr = pk != null ? String(pk) : "";

    const data = {
      releaseId: idValStr,
      customer: {
        cnpj: row.customer_cnpj ?? "",
        internalCode: row.customer_internal_code ?? "",
        companyName: row.customer_company_name ?? "",
        tradeName: row.customer_trade_name ?? "",
        alias: row.customer_alias ?? "",
        municipality: row.customer_municipality ?? "",
        state: row.customer_state ?? "",
        country: row.customer_country ?? "",
      },
      customerReleaseId: row.customer_release_id ?? "",
      releaseDate: toDateStr(row.release_date),
      fileName: row.file_name ?? "",
      receiptFileName: row.receipt_file_name ?? "",
      arrivalTimestamp: toIso(row.arrival_timestamp),
      itemsQty: Number(row.items_qty) || 0,
      deliveriesQty: Number(row.deliveries_qty) || 0,
      releaseStatus: row.release_status ?? "",
    };

    return res.status(200).json({
      success: true,
      data,
    });
  } catch (err) {
    const msg = err && (err.message || err.code || String(err));
    console.error("GET /releases/:releaseId/header:", msg);
    if (err && err.stack) console.error(err.stack);
    if (!res.headersSent) {
      return res.status(500).json({
        success: false,
        error: "Internal server error",
        message: msg || "Erro desconhecido",
      });
    }
  }
});

/**
 * GET /releases/:releaseId/items
 * Lista itens do release com paginação e filtros.
 * Query: page (default 1), pageSize (default 30), sort (+customerPN|+customerPurchaseOrder|+programId ou -...), customerPN, customerPurchaseOrder, programId.
 * 200: SuccessResponse com PaginatedResponse<ReleaseItem>
 * 400: releaseId inválido (ErrorResponse)
 * 500: Erro no servidor (ErrorResponse)
 */
router.get("/:releaseId/items", async (req, res) => {
  try {
    const releaseIdRaw = (req.params?.releaseId ?? "").toString().trim();
    if (!releaseIdRaw) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "releaseId é obrigatório.",
      });
    }
    const releaseIdNum = parseInt(releaseIdRaw, 10);
    if (Number.isNaN(releaseIdNum) || releaseIdNum < 1 || String(releaseIdNum) !== releaseIdRaw) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "releaseId deve ser um identificador válido do release.",
      });
    }

    const query = req.query || {};
    const page = Math.max(1, parseInt(query.page, 10) || 1);
    const pageSize = Math.min(100, Math.max(1, parseInt(query.pageSize, 10) || 30));
    const sortParamRaw = String(query.sort ?? "+customerPN").trim() || "+customerPN";
    const sortParam = sortParamRaw.includes(",") ? sortParamRaw.split(",")[0].trim() || "+customerPN" : sortParamRaw;
    const customerPNFilter = (query.customerPN ?? "").toString().trim();
    const customerPurchaseOrderFilter = (query.customerPurchaseOrder ?? "").toString().trim();
    const programIdFilter = (query.programId ?? "").toString().trim();

    const isDesc = sortParam.startsWith("-");
    const sortFieldRaw = (isDesc ? sortParam.slice(1) : sortParam.replace(/^\+/, "")).trim() || "customerPN";
    const sortMap = {
      customerpn: "ri.customer_pn",
      customerpurchaseorder: "ri.customer_purchase_order",
      programid: "ri.program_id",
    };
    const orderBy = sortMap[sortFieldRaw.toLowerCase()] || "ri.customer_pn";
    const orderDir = isDesc ? "DESC" : "ASC";
    const offset = Math.max(0, (page - 1) * pageSize);

    const conditions = ["ri.release_id = ?"];
    const countParams = [releaseIdNum];
    if (customerPNFilter) {
      conditions.push("ri.customer_pn = ?");
      countParams.push(customerPNFilter);
    }
    if (customerPurchaseOrderFilter) {
      conditions.push("ri.customer_purchase_order = ?");
      countParams.push(customerPurchaseOrderFilter);
    }
    if (programIdFilter) {
      conditions.push("ri.program_id = ?");
      countParams.push(programIdFilter);
    }
    const whereClause = `WHERE ${conditions.join(" AND ")}`;

    const countRows = await executarQueryMySQL(
      CLIENT_PREFIX,
      `SELECT COUNT(*) AS total FROM release_items ri ${whereClause}`,
      countParams
    );
    const firstCount = Array.isArray(countRows) && countRows.length > 0 ? countRows[0] : null;
    const totalRecords = Math.max(0, parseInt(firstCount?.total ?? firstCount?.TOTAL ?? 0, 10) || 0);

    const limitNum = Math.min(100, Math.max(1, pageSize));
    const offsetNum = Math.max(0, offset);
    const rows = await executarQueryMySQL(
      CLIENT_PREFIX,
      `SELECT ri.id, ri.sequence, ri.customer_purchase_order, ri.program_id, ri.program_date,
              ri.customer_pn, ri.technical_revision, ri.supplier_pn, ri.unit_of_measure,
              ri.last_received_date, ri.last_received_qty, ri.last_invoice_number, ri.last_invoice_series,
              ri.last_invoice_date, ri.last_acc_qty, ri.acc_start_date, ri.contact_person, ri.notes
       FROM release_items ri
       ${whereClause}
       ORDER BY ${orderBy} ${orderDir}
       LIMIT ${limitNum} OFFSET ${offsetNum}`,
      countParams
    );

    const toDateStr = (v) => (v instanceof Date ? v.toISOString().slice(0, 10) : v != null ? String(v).slice(0, 10) : null);
    const itemList = Array.isArray(rows) ? rows : [];
    const records = itemList.map((row) => {
      const pk = row.id ?? row.ID ?? row.Id;
      const releaseItemIdStr = pk != null ? String(pk) : "";
      return {
        releaseItemId: releaseItemIdStr,
        sequence: row.sequence != null ? Number(row.sequence) : null,
        customerPurchaseOrder: row.customer_purchase_order ?? null,
        programId: row.program_id ?? null,
        programDate: toDateStr(row.program_date),
        customerPN: row.customer_pn ?? null,
        technicalRevision: row.technical_revision ?? null,
        supplierPN: row.supplier_pn ?? null,
        unitOfMeasure: row.unit_of_measure ?? null,
        lastReceivedDate: toDateStr(row.last_received_date),
        lastReceivedQty: row.last_received_qty != null ? Number(row.last_received_qty) : null,
        lastInvoiceNumber: row.last_invoice_number ?? null,
        lastInvoiceSeries: row.last_invoice_series ?? null,
        lastInvoiceDate: toDateStr(row.last_invoice_date),
        lastAccQty: row.last_acc_qty != null ? Number(row.last_acc_qty) : null,
        accStartDate: toDateStr(row.acc_start_date),
        contactPerson: row.contact_person ?? null,
        notes: row.notes ?? null,
      };
    });

    const totalPages = Math.max(1, Number.isFinite(totalRecords) && pageSize > 0 ? Math.ceil(totalRecords / pageSize) : 1);
    const searchParams = {};
    let hasSearchParams = false;
    if (customerPNFilter) { searchParams.customerPN = customerPNFilter; hasSearchParams = true; }
    if (customerPurchaseOrderFilter) { searchParams.customerPurchaseOrder = customerPurchaseOrderFilter; hasSearchParams = true; }
    if (programIdFilter) { searchParams.programId = programIdFilter; hasSearchParams = true; }

    return res.status(200).json({
      success: true,
      data: {
        page,
        pageSize,
        sort: sortParam,
        searchParams: hasSearchParams ? searchParams : null,
        totalRecords,
        totalPages,
        records,
      },
    });
  } catch (err) {
    const msg = err && (err.message || err.code || String(err));
    console.error("GET /releases/:releaseId/items:", msg);
    if (err && err.stack) console.error(err.stack);
    if (!res.headersSent) {
      return res.status(500).json({
        success: false,
        error: "Internal server error",
        message: msg || "Erro desconhecido",
      });
    }
  }
});

/**
 * GET /releases/:releaseId/items/:releaseItemId/deliveries
 * Lista entregas do item do release. Lista ordenada por sequence crescente.
 * 200: SuccessResponse com array de ReleaseItemDelivery
 * 400: releaseId ou releaseItemId inválidos / item não pertence ao release (ErrorResponse)
 * 500: Erro no servidor (ErrorResponse)
 */
router.get("/:releaseId/items/:releaseItemId/deliveries", async (req, res) => {
  try {
    const releaseIdRaw = (req.params.releaseId ?? "").toString().trim();
    const releaseItemIdRaw = (req.params.releaseItemId ?? "").toString().trim();
    if (!releaseIdRaw) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "releaseId é obrigatório.",
      });
    }
    if (!releaseItemIdRaw) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "releaseItemId é obrigatório.",
      });
    }
    const releaseIdNum = parseInt(releaseIdRaw, 10);
    const releaseItemIdNum = parseInt(releaseItemIdRaw, 10);
    if (Number.isNaN(releaseIdNum) || releaseIdNum < 1 || String(releaseIdNum) !== releaseIdRaw) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "releaseId deve ser um identificador válido do release.",
      });
    }
    if (Number.isNaN(releaseItemIdNum) || releaseItemIdNum < 1 || String(releaseItemIdNum) !== releaseItemIdRaw) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "releaseItemId deve ser um identificador válido do item.",
      });
    }

    const itemCheck = await executarQueryMySQL(
      CLIENT_PREFIX,
      "SELECT id FROM release_items WHERE id = ? AND release_id = ? LIMIT 1",
      [releaseItemIdNum, releaseIdNum]
    );
    if (!Array.isArray(itemCheck) || itemCheck.length === 0) {
      return res.status(400).json({
        success: false,
        error: "Invalid parameters",
        message: "Item não encontrado ou não pertence ao release informado.",
      });
    }

    const rows = await executarQueryMySQL(
      CLIENT_PREFIX,
      `SELECT id, sequence, type, due_date, delivery_time, qty, acc_qty
       FROM release_deliveries
       WHERE item_id = ?
       ORDER BY sequence ASC`,
      [releaseItemIdNum]
    );

    const toDateStr = (v) => (v instanceof Date ? v.toISOString().slice(0, 10) : v != null ? String(v).slice(0, 10) : null);
    const toTimeStr = (v) => {
      if (v == null) return null;
      if (typeof v === "string") return v;
      if (v instanceof Date) return v.toTimeString().slice(0, 8);
      return String(v);
    };
    const list = Array.isArray(rows) ? rows : [];
    const data = list.map((row) => {
      const pk = row.id ?? row.ID ?? row.Id;
      return {
        releaseDeliveryId: pk != null ? String(pk) : "",
        sequence: row.sequence != null ? String(row.sequence) : null,
        type: row.type ?? null,
        dueDate: toDateStr(row.due_date),
        deliveryTime: toTimeStr(row.delivery_time),
        qty: row.qty != null ? Number(row.qty) : null,
        accQty: row.acc_qty != null ? Number(row.acc_qty) : null,
      };
    });

    return res.status(200).json({
      success: true,
      data,
    });
  } catch (err) {
    const msg = err && (err.message || err.code || String(err));
    console.error("GET /releases/:releaseId/items/:releaseItemId/deliveries:", msg);
    if (err && err.stack) console.error(err.stack);
    if (!res.headersSent) {
      return res.status(500).json({
        success: false,
        error: "Internal server error",
        message: msg || "Erro desconhecido",
      });
    }
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
