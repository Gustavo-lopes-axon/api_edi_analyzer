const mysql = require("mysql2/promise");

require("dotenv").config();

/** @type {Map<string, import("mysql2/promise").Pool>} */
const pools = new Map();

/**
 * Returns (and caches) a connection pool for the given client prefix.
 * Reads {PREFIX}_MYSQL_HOST, _PORT, _USER, _PASSWORD, _DATABASE from env.
 * @param {string} clientPrefix  e.g. "PEDERTRACTOR"
 * @returns {import("mysql2/promise").Pool}
 */
function getPool(clientPrefix) {
  const prefix = clientPrefix.toUpperCase();

  if (pools.has(prefix)) return pools.get(prefix);

  const host = process.env[`${prefix}_MYSQL_HOST`];
  const port = parseInt(process.env[`${prefix}_MYSQL_PORT`] ?? "3306", 10);
  const user = process.env[`${prefix}_MYSQL_USER`];
  const password = process.env[`${prefix}_MYSQL_PASSWORD`] ?? "";
  const database = process.env[`${prefix}_MYSQL_DATABASE`];

  if (!host || !user || !database) {
    throw new Error(
      `Variáveis de ambiente MySQL ausentes para o prefixo "${prefix}". ` +
        `Verifique ${prefix}_MYSQL_HOST, ${prefix}_MYSQL_USER e ${prefix}_MYSQL_DATABASE no .env`
    );
  }

  const pool = mysql.createPool({
    host,
    port,
    user,
    password,
    database,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    timezone: "Z",
    decimalNumbers: true,
  });

  pools.set(prefix, pool);
  return pool;
}

/**
 * Executes a parameterized query and returns all rows.
 * @param {string} clientPrefix
 * @param {string} sql
 * @param {Array} [params]
 * @returns {Promise<Array>}
 */
async function executarQueryMySQL(clientPrefix, sql, params = []) {
  const pool = getPool(clientPrefix);
  const [rows] = await pool.execute(sql, params);
  return rows;
}

/**
 * Acquires a connection from the pool (for manual transactions).
 * Caller is responsible for calling connection.release() when done.
 * @param {string} clientPrefix
 * @returns {Promise<import("mysql2/promise").PoolConnection>}
 */
async function getConnectionMySQL(clientPrefix) {
  const pool = getPool(clientPrefix);
  return pool.getConnection();
}

module.exports = { executarQueryMySQL, getConnectionMySQL };
