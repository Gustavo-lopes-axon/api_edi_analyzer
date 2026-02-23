const express = require("express");
const path = require("path");

const router = express.Router();

require("dotenv").config({ path: path.join(__dirname, "../../../../.env") });
const { executarQueryFirebird } = require("../../../../engines/firebirdClient.js");
const { getConnectionMySQL } = require("../../../../engines/mysqlClient.js");

const CLIENT_PREFIX = "PEDERTRACTOR";
/** View de pedidos em carteira/backlog (ajuste conforme o nome no Firebird) */
const VIEW_BACKLOG = "VW_AXON_PEDIDO";

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
  return v === undefined || v === null ? "" : String(v);
}
function num(v) {
  return v === undefined || v === null ? 0 : Number(v);
}

/**
 * Agrupa linhas flat da view VW_AXON_PEDIDO em estrutura Backlog.
 *
 * Colunas da view usadas:
 *   ORDEM_COMPRA          → customerPurchaseOrder / supplierSalesOrder
 *   COD_ITEM              → customerPN
 *   DATA_ENTREGA          → dueDate da entrega
 *   DATA_NEGOCIADA        → deliveryTime (hora negociada, se preenchida)
 *   QUANTIDADE_PEDIDO     → qty da entrega
 *   SITUACAO              → tipo da entrega (firm / planning)
 *   DATAHORA_REGISTRO     → usado para ordenação de sequência
 *
 * Campos sem coluna direta na view (retornam null/0 por padrão):
 *   customerTechnicalRevision, supplierPN, suppierTechnicalRevision,
 *   lifecycleStage, category, leadTime
 */
function rowsToBacklog(customerCode, rows) {
  if (!rows || rows.length === 0) {
    return { customerCode, orders: [] };
  }

  const orderMap = new Map(); // ORDEM_COMPRA -> order

  for (const row of rows) {
    // --- pedido ---
    const customerPurchaseOrder = str(get(row, "ORDEM_COMPRA", "CUSTOMERPURCHASEORDER", "CUSTOMER_PURCHASE_ORDER"));
    const supplierSalesOrder = str(get(row, "SUPPLIERSALESORDER", "SUPPLIER_SALES_ORDER")) || customerPurchaseOrder;

    if (!orderMap.has(customerPurchaseOrder)) {
      orderMap.set(customerPurchaseOrder, { customerPurchaseOrder, supplierSalesOrder, items: [] });
    }
    const order = orderMap.get(customerPurchaseOrder);

    // --- item ---
    const customerPN = str(get(row, "COD_ITEM", "CUSTOMERPN", "CODIGO_ITEM", "PART_NUMBER"));
    let item = order.items.find((i) => i.customerPN === customerPN);
    if (!item) {
      item = {
        customerPN,
        customerTechnicalRevision: str(get(row, "CUSTOMERTECHNICALREVISION", "REVISAO_CLIENTE", "REV_CLIENTE")) || null,
        supplierPN: str(get(row, "SUPPLIERPN", "CODIGO_FORNECEDOR")) || null,
        suppierTechnicalRevision: str(get(row, "SUPPLIERTECHNICALREVISION", "REVISAO_FORNECEDOR")) || null,
        lifecycleStage: get(row, "LIFECYCLESTAGE", "ESTAGIO_VIDA") != null
          ? str(get(row, "LIFECYCLESTAGE", "ESTAGIO_VIDA")) : null,
        category: get(row, "CATEGORY", "CATEGORIA") != null
          ? str(get(row, "CATEGORY", "CATEGORIA")) : null,
        leadTime: num(get(row, "LEADTIME", "LEAD_TIME", "PRAZO")),
        deliveries: [],
      };
      order.items.push(item);
    }

    // --- entrega ---
    const rawDueDate = get(row, "DATA_ENTREGA", "DUEDATE", "DUE_DATE");
    const dueDateStr = rawDueDate instanceof Date
      ? rawDueDate.toISOString().slice(0, 10)
      : rawDueDate != null ? String(rawDueDate).slice(0, 10) : null;

    const rawNegociada = get(row, "DATA_NEGOCIADA");
    const deliveryTime = rawNegociada instanceof Date
      ? rawNegociada.toISOString().slice(11, 19)
      : rawNegociada != null ? String(rawNegociada) : null;

    // SITUACAO: "F"/"FIRM" → firm, qualquer outra coisa → planning
    const situacao = str(get(row, "SITUACAO", "TYPE", "TIPO", "DELIVERY_TYPE")).toUpperCase();
    const deliveryType = situacao === "F" || situacao === "FIRM" ? "firm" : "planning";

    const qty = num(get(row, "QUANTIDADE_PEDIDO", "QTY", "QUANTIDADE", "QTDE"));

    item.deliveries.push({
      sequence: item.deliveries.length * 10 + 10,
      type: deliveryType,
      dueDate: dueDateStr,
      deliveryTime,
      qty,
    });
  }

  const orders = Array.from(orderMap.values());
  orders.forEach((o) => {
    o.items.forEach((i) => {
      i.deliveries.sort((a, b) => (a.dueDate ?? "").localeCompare(b.dueDate ?? ""));
      // Renumber sequences after sort
      i.deliveries.forEach((d, idx) => { d.sequence = (idx + 1) * 10; });
    });
  });

  return { customerCode, orders };
}

/**
 * GET /orders/backlog
 * Busca lista de pedidos em carteira (backlog) por código interno do cliente.
 * Query obrigatório: customerCode.
 * Retorna Backlog com "orders" vazio se nenhum pedido for encontrado.
 */
router.get("/backlog", async (req, res) => {
  try {
    const customerCode = (req.query.customerCode ?? req.query.customer_code ?? "").toString().trim();

    if (!customerCode) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: "O parâmetro customerCode é obrigatório.",
      });
    }

    const safeCode = escapeSql(customerCode);
    let data = { customerCode, orders: [] };

    try {
      const numericCode = parseInt(safeCode, 10);
      const whereClause = isNaN(numericCode)
        ? `TRIM(CAST(COD_CLIENTE AS VARCHAR(20))) = '${safeCode}'`
        : `COD_CLIENTE = ${numericCode}`;
      const sql = `
        SELECT *
        FROM ${VIEW_BACKLOG}
        WHERE ${whereClause}
        ORDER BY ORDEM_COMPRA, COD_ITEM, DATA_ENTREGA
      `.trim();
      const rows = await executarQueryFirebird(CLIENT_PREFIX, sql);
      data = rowsToBacklog(customerCode, rows);
    } catch (_) {
      // View inexistente ou erro: retorna orders vazio
    }

    return res.status(200).json({
      success: true,
      data,
    });
  } catch (err) {
    console.error("GET /orders/backlog:", err.message);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: err.message,
    });
  }
});

/**
 * POST /orders
 * Insere um novo pedido (upsert por customerCode + customerPurchaseOrder).
 * Corpo: { customerCode, customerPurchaseOrder } (ambos obrigatórios).
 * Retorna 201 Created com InsertionResponse { recordId }.
 */
router.post("/", async (req, res) => {
  try {
    const body = req.body ?? {};
    const customerCode = (body.customerCode ?? body.customer_code ?? "").toString().trim();
    const customerPurchaseOrder = (body.customerPurchaseOrder ?? body.customer_purchase_order ?? "").toString().trim();

    const missing = [];
    if (!customerCode) missing.push("customerCode");
    if (!customerPurchaseOrder) missing.push("customerPurchaseOrder");

    if (missing.length > 0) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: `Campos obrigatórios: ${missing.join(", ")}`,
      });
    }

    const conn = await getConnectionMySQL(CLIENT_PREFIX);
    let recordId;
    try {
      await conn.execute(
        `INSERT INTO orders (customer_code, customer_purchase_order)
         VALUES (?, ?)
         ON DUPLICATE KEY UPDATE customer_purchase_order = VALUES(customer_purchase_order)`,
        [customerCode, customerPurchaseOrder]
      );
      const [[row]] = await conn.execute(
        "SELECT id FROM orders WHERE customer_code = ? AND customer_purchase_order = ?",
        [customerCode, customerPurchaseOrder]
      );
      recordId = String(row.id);
    } finally {
      conn.release();
    }

    return res.status(201).json({
      success: true,
      data: { recordId },
    });
  } catch (err) {
    console.error("POST /orders:", err.message);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: err.message,
    });
  }
});

/**
 * POST /orders/items
 * Insere um item (com entregas) em um pedido existente.
 * Corpo: { customerCode, customerPurchaseOrder, item } (todos obrigatórios).
 * Após inserir, marca release_analysis_items.is_implemented = 1 para o itemAnalysisId informado.
 * Retorna 201 Created com InsertionResponse { recordId }.
 */
router.post("/items", async (req, res) => {
  try {
    const body = req.body ?? {};
    const customerCode = (body.customerCode ?? body.customer_code ?? "").toString().trim();
    const customerPurchaseOrder = (body.customerPurchaseOrder ?? body.customer_purchase_order ?? "").toString().trim();
    const item = body.item;

    const missing = [];
    if (!customerCode) missing.push("customerCode");
    if (!customerPurchaseOrder) missing.push("customerPurchaseOrder");
    if (item === undefined) missing.push("item");

    if (missing.length > 0) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: `Campos obrigatórios: ${missing.join(", ")}`,
      });
    }

    if (!item || typeof item !== "object" || Array.isArray(item)) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: "item deve ser um objeto (OrderItemToImplement com entregas).",
      });
    }

    const deliveries = item.deliveries;
    if (deliveries !== undefined && !Array.isArray(deliveries)) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: "item.deliveries deve ser um array (pode ser vazio).",
      });
    }

    const toDate = (v) => {
      if (v == null) return null;
      const s = String(v).trim();
      return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : null;
    };
    const toNum = (v) => (v == null ? null : (isNaN(Number(v)) ? null : Number(v)));

    const conn = await getConnectionMySQL(CLIENT_PREFIX);
    let recordId;
    try {
      await conn.beginTransaction();

      // 1. Ensure the order exists (upsert)
      await conn.execute(
        `INSERT INTO orders (customer_code, customer_purchase_order)
         VALUES (?, ?)
         ON DUPLICATE KEY UPDATE customer_purchase_order = VALUES(customer_purchase_order)`,
        [customerCode, customerPurchaseOrder]
      );
      const [[orderRow]] = await conn.execute(
        "SELECT id FROM orders WHERE customer_code = ? AND customer_purchase_order = ?",
        [customerCode, customerPurchaseOrder]
      );
      const orderId = orderRow.id;

      // 2. Insert order item
      const itemAnalysisId = item.itemAnalysisId ?? item.item_analysis_id ?? null;
      const analysisResult = item.analysisResult ?? item.analysis_result ?? null;

      const [itemResult] = await conn.execute(
        `INSERT INTO order_items
           (order_id, customer_pn, customer_technical_revision, supplier_pn,
            supplier_technical_revision, lifecycle_stage, category, lead_time,
            item_analysis_id, analysis_result_json)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          orderId,
          item.customerPN ?? item.customer_pn ?? null,
          item.customerTechnicalRevision ?? item.customer_technical_revision ?? null,
          item.supplierPN ?? item.supplier_pn ?? null,
          item.supplierTechnicalRevision ?? item.supplier_technical_revision ?? null,
          item.lifecycleStage ?? item.lifecycle_stage ?? null,
          item.category ?? null,
          toNum(item.leadTime ?? item.lead_time),
          itemAnalysisId != null ? String(itemAnalysisId) : null,
          analysisResult != null ? JSON.stringify(analysisResult) : null,
        ]
      );
      const orderItemId = itemResult.insertId;
      recordId = String(orderItemId);

      // 3. Insert deliveries
      const deliveryList = Array.isArray(deliveries) ? deliveries : [];
      for (const d of deliveryList) {
        await conn.execute(
          `INSERT INTO order_deliveries (order_item_id, sequence, type, due_date, delivery_time, qty)
           VALUES (?, ?, ?, ?, ?, ?)`,
          [
            orderItemId,
            toNum(d.sequence),
            d.type ?? null,
            toDate(d.dueDate ?? d.due_date),
            d.deliveryTime ?? d.delivery_time ?? null,
            toNum(d.qty),
          ]
        );
      }

      // 4. Mark analysis item as implemented
      if (itemAnalysisId != null) {
        await conn.execute(
          "UPDATE release_analysis_items SET is_implemented = 1 WHERE id = ?",
          [String(itemAnalysisId)]
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
      data: { recordId },
    });
  } catch (err) {
    console.error("POST /orders/items:", err.message);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: err.message,
    });
  }
});

/**
 * PUT /orders/deliveries
 * Atualiza as entregas de um item a partir de uma análise.
 * Corpo: { customerCode, customerPurchaseOrder, item } (todos obrigatórios; item = OrderItemToImplement com deliveries).
 * Após a atualização, o campo isImplemented da análise do item deve ser alterado para true (persistência).
 * Retorna 200 com UpdateResponse { numberOfRecordsUpdated } (ex.: número de entregas atualizadas).
 */
router.put("/deliveries", async (req, res) => {
  try {
    const body = req.body ?? {};
    const customerCode = (body.customerCode ?? body.customer_code ?? "").toString().trim();
    const customerPurchaseOrder = (body.customerPurchaseOrder ?? body.customer_purchase_order ?? "").toString().trim();
    const item = body.item;

    const missing = [];
    if (!customerCode) missing.push("customerCode");
    if (!customerPurchaseOrder) missing.push("customerPurchaseOrder");
    if (item === undefined) missing.push("item");

    if (missing.length > 0) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: `Campos obrigatórios: ${missing.join(", ")}`,
      });
    }

    if (!item || typeof item !== "object" || Array.isArray(item)) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: "item deve ser um objeto (OrderItemToImplement com entregas).",
      });
    }

    const deliveries = item.deliveries;
    if (deliveries !== undefined && !Array.isArray(deliveries)) {
      return res.status(400).json({
        success: false,
        error: "Parâmetros inválidos",
        message: "item.deliveries deve ser um array (pode ser vazio).",
      });
    }

    const toDate = (v) => {
      if (v == null) return null;
      const s = String(v).trim();
      return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : null;
    };
    const toNum = (v) => (v == null ? null : (isNaN(Number(v)) ? null : Number(v)));

    const customerPN = (item.customerPN ?? item.customer_pn ?? "").toString().trim();
    const itemAnalysisId = item.itemAnalysisId ?? item.item_analysis_id ?? null;
    const analysisResult = item.analysisResult ?? item.analysis_result ?? null;
    const deliveryList = Array.isArray(deliveries) ? deliveries : [];

    const conn = await getConnectionMySQL(CLIENT_PREFIX);
    let numberOfRecordsUpdated = 0;
    try {
      await conn.beginTransaction();

      // 1. Find the order_item by order + customerPN
      const [[orderItemRow]] = await conn.execute(
        `SELECT oi.id FROM order_items oi
         JOIN orders o ON o.id = oi.order_id
         WHERE o.customer_code = ?
           AND o.customer_purchase_order = ?
           AND oi.customer_pn = ?
         LIMIT 1`,
        [customerCode, customerPurchaseOrder, customerPN]
      );

      if (!orderItemRow) {
        await conn.rollback();
        return res.status(400).json({
          success: false,
          error: "Não encontrado",
          message: `Item "${customerPN}" não encontrado no pedido "${customerPurchaseOrder}" do cliente "${customerCode}".`,
        });
      }

      const orderItemId = orderItemRow.id;

      // 2. Replace deliveries: delete existing, insert new ones
      await conn.execute("DELETE FROM order_deliveries WHERE order_item_id = ?", [orderItemId]);

      for (const d of deliveryList) {
        await conn.execute(
          `INSERT INTO order_deliveries (order_item_id, sequence, type, due_date, delivery_time, qty)
           VALUES (?, ?, ?, ?, ?, ?)`,
          [
            orderItemId,
            toNum(d.sequence),
            d.type ?? null,
            toDate(d.dueDate ?? d.due_date),
            d.deliveryTime ?? d.delivery_time ?? null,
            toNum(d.qty),
          ]
        );
      }
      numberOfRecordsUpdated = deliveryList.length;

      // 3. Update analysis_result_json on the item
      if (analysisResult != null) {
        await conn.execute(
          "UPDATE order_items SET analysis_result_json = ? WHERE id = ?",
          [JSON.stringify(analysisResult), orderItemId]
        );
      }

      // 4. Mark analysis item as implemented
      if (itemAnalysisId != null) {
        await conn.execute(
          "UPDATE release_analysis_items SET is_implemented = 1 WHERE id = ?",
          [String(itemAnalysisId)]
        );
      }

      await conn.commit();
    } catch (err) {
      try { await conn.rollback(); } catch (_) {}
      throw err;
    } finally {
      conn.release();
    }

    return res.status(200).json({
      success: true,
      data: { numberOfRecordsUpdated },
    });
  } catch (err) {
    console.error("PUT /orders/deliveries:", err.message);
    return res.status(500).json({
      success: false,
      error: "Erro no servidor",
      message: err.message,
    });
  }
});

module.exports = router;
