const Firebird = require("node-firebird");
const { getFirebirdConfig } = require("../utils/config.js");

const CONEXAO_TIMEOUT_MS = 20000;

/**
 * @param {string} clientPrefix
 * @param {string} sql
 * @returns {Promise<Array>}
 */
async function executarQueryFirebird(clientPrefix, sql) {
  const options = getFirebirdConfig(clientPrefix);

  const connectPromise = new Promise((resolve, reject) => {
    Firebird.attach(options, function (err, db) {
      if (err) {
        return reject(new Error(`Falha na conexão Firebird: ${err.message}`));
      }
      resolve(db);
    });
  });

  const timeoutPromise = new Promise((_, reject) => {
    setTimeout(() => {
      reject(
        new Error(
          `Timeout de conexão (${CONEXAO_TIMEOUT_MS / 1000}s). Verifique IP (${
            options.host
          }:${options.port}), firewall e variáveis ${clientPrefix}_FB_* no .env`
        )
      );
    }, CONEXAO_TIMEOUT_MS);
  });

  const db = await Promise.race([connectPromise, timeoutPromise]);

  return new Promise((resolve, reject) => {
    db.query(sql, function (err, result) {
      db.detach();
      if (err) {
        return reject(new Error(`Erro na query: ${err.message}`));
      }
      resolve(result);
    });
  });
}

module.exports = { executarQueryFirebird };
