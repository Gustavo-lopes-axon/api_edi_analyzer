const Firebird = require("node-firebird");
const { getFirebirdConfig } = require("../utils/config.js");

/**
 * @param {string} clientPrefix
 * @param {string} sql
 * @returns {Promise<Array>}
 */
async function executarQueryFirebird(clientPrefix, sql) {
  const options = getFirebirdConfig(clientPrefix);

  return new Promise((resolve, reject) => {
    // Abre a conexão
    Firebird.attach(options, function (err, db) {
      if (err) {
        return reject(new Error(`Falha na conexão Firebird: ${err.message}`));
      }

      db.query(sql, function (err, result) {
        if (err) {
          db.detach();
          return reject(new Error(`Erro na query: ${err.message}`));
        }

        db.detach();
        resolve(result);
      });
    });
  });
}

module.exports = { executarQueryFirebird };
