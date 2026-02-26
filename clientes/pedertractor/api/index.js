/**
 * API PEDERTRACTOR – servidor Express (dados direto do Firebird).
 *
 * Uso:
 *   node clientes/pedertractor/api/index.js
 *   PORT=3001 node clientes/pedertractor/api/index.js
 *
 * Autenticação (Basic Auth):
 *   Credenciais em .env: PEDERTRACTOR_API_USER e PEDERTRACTOR_API_PASSWORD.
 *   Token = Base64 de "usuario:senha". Exemplo: node -e "console.log(Buffer.from('demo:p@55w0rd').toString('base64'))"
 *   Header: Authorization: Basic <token>
 *
 * Variáveis de ambiente (prefixo PEDERTRACTOR_):
 *   FB_*, MYSQL_*, API_USER, API_PASSWORD
 */

const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "../../../.env") });
const express = require("express");
const customersRouter = require("./routes/customers.js");
const itemsRouter = require("./routes/items.js");
const issuedInvoicesRouter = require("./routes/issued-invoices.js");
const receivedInvoicesRouter = require("./routes/received-invoices.js");
const releasesRouter = require("./routes/releases.js");
const analysisRouter = require("./routes/analysis.js");
const ordersRouter = require("./routes/orders.js");

const app = express();
const PORT = process.env.PORT || 3001;

app.use(express.json());

/**
 * Validação por login e senha via Basic Auth.
 * Header esperado: Authorization: Basic <base64(usuario:senha)>
 * Credenciais em .env: PEDERTRACTOR_API_USER e PEDERTRACTOR_API_PASSWORD.
 * Rotas em /health não exigem autenticação.
 */
const apiUser = process.env.PEDERTRACTOR_API_USER;
const apiPassword = process.env.PEDERTRACTOR_API_PASSWORD;
const authEnabled = apiUser != null && apiUser !== "" && apiPassword != null;

if (!authEnabled) {
  console.warn("[API] PEDERTRACTOR_API_USER ou PEDERTRACTOR_API_PASSWORD não definidos; requisições não serão autenticadas.");
}

app.use((req, res, next) => {
  if (req.path === "/health") return next();
  if (!authEnabled) return next();

  const authHeader = req.headers.authorization;
  if (!authHeader || !/^Basic\s+/i.test(authHeader)) {
    return res.status(401).json({
      success: false,
      error: "Não autorizado",
      message: "Header Authorization: Basic <token> é obrigatório.",
    });
  }

  const base64 = authHeader.replace(/^Basic\s+/i, "").trim();
  let decoded;
  try {
    decoded = Buffer.from(base64, "base64").toString("utf8");
  } catch (_) {
    return res.status(401).json({
      success: false,
      error: "Não autorizado",
      message: "Token Base64 inválido.",
    });
  }

  const colon = decoded.indexOf(":");
  const user = colon >= 0 ? decoded.slice(0, colon) : decoded;
  const password = colon >= 0 ? decoded.slice(colon + 1) : "";

  if (user !== apiUser || password !== apiPassword) {
    return res.status(401).json({
      success: false,
      error: "Não autorizado",
      message: "Login ou senha inválidos.",
    });
  }

  next();
});

app.use("/customers", customersRouter);
app.use("/items", itemsRouter);
app.use("/issued-invoices", issuedInvoicesRouter);
app.use("/received-invoices", receivedInvoicesRouter);
app.use("/releases", releasesRouter);
app.use("/analysis", analysisRouter);
app.use("/orders", ordersRouter);

app.use("/api/axon/prox/v1/customers", customersRouter);
app.use("/api/axon/prox/v1/items", itemsRouter);
app.use("/api/axon/prox/v1/issued-invoices", issuedInvoicesRouter);
app.use("/api/axon/prox/v1/received-invoices", receivedInvoicesRouter);
app.use("/api/axon/prox/v1/releases", releasesRouter);
app.use("/api/axon/prox/v1/analysis", analysisRouter);
app.use("/api/axon/prox/v1/orders", ordersRouter);

app.get("/health", (req, res) => {
  res.json({ status: "ok", client: "PEDERTRACTOR" });
});

function tryListen(port) {
  const server = app.listen(port, () => {
    console.log(`API PEDERTRACTOR rodando em http://localhost:${port}`);
    console.log("  GET /customers/:cnpj (Firebird)");
    console.log("  GET /items?customerPN=... (Firebird)");
    console.log("  GET /issued-invoices?customerCode=...&customerPurchaseOrder=...&customerPN=...&startDate=...&endDate=...");
    console.log("  GET /received-invoices?customerCode=...&customerPN=...&startDate=...&endDate=...");
    console.log("  GET /releases/status?customerCnpj=...&customerReleaseId=...&releaseDate=...");
    console.log("  POST /releases/status (body: releaseId, timestamp, releaseStatus)");
    console.log("  POST /releases (body: release JSON)");
    console.log("  POST /analysis (body: releaseId, force, analysisVersion, analysisConfigs)");
    console.log("  GET /analysis/items/last-firm-date?customerCnpj=...&customerPurchaseOrder=...&customerPN=...");
    console.log("  POST /analysis/items (body: itemAnalysis)");
    console.log("  PUT /analysis/items/implementation-comments (body: itemAnalysisId, comments)");
    console.log("  PUT /analysis/duration-and-totals (body: releaseAnalysisId, analysisDuration, totals)");
    console.log("  POST /analysis/status (body: releaseAnalysisId, timestamp, analysisStatus)");
    console.log("  GET /orders/backlog?customerCode=...");
    console.log("  POST /orders (body: customerCode, customerPurchaseOrder)");
    console.log("  POST /orders/items (body: customerCode, customerPurchaseOrder, item)");
    console.log("  PUT /orders/deliveries (body: customerCode, customerPurchaseOrder, item)");
    console.log("  GET /api/axon/prox/v1/items | issued-invoices | received-invoices | releases | analysis | orders");
    console.log("  GET /health");
  });
  server.on("error", (err) => {
    if (err.code === "EADDRINUSE") {
      console.warn(`Porta ${port} em uso, tentando ${port + 1}...`);
      tryListen(port + 1);
    } else {
      throw err;
    }
  });
}

tryListen(PORT);
