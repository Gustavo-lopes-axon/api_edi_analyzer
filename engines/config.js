const getMssqlConfig = (clientPrefix) => {
    const prefix = clientPrefix.toUpperCase();

    const config = {
        user: process.env[`${prefix}_MSSQL_USER`],
        password: process.env[`${prefix}_MSSQL_PASSWORD`],
        server: process.env[`${prefix}_MSSQL_SERVER`],
        database: process.env[`${prefix}_MSSQL_DATABASE`],
        options: {
            encrypt: process.env[`${prefix}_MSSQL_ENCRYPT`] === 'true',
            trustServerCertificate: process.env[`${prefix}_MSSQL_TRUST_CERT`] === 'true',
        }
    };

    if (!config.server || !config.database) {
        throw new Error(`Configuração MSSQL crítica faltando para o cliente ${prefix}. Verifique o .env.`);
    }

    return config;
};

const getFirebirdConfig = (clientPrefix) => {
    const prefix = clientPrefix.toUpperCase();
    const config = {
        host: process.env[`${prefix}_FB_HOST`],
        port: parseInt(process.env[`${prefix}_FB_PORT`]),
        database: process.env[`${prefix}_FB_DATABASE`],
        user: process.env[`${prefix}_FB_USER`],
        password: process.env[`${prefix}_FB_PASSWORD`],
        role: process.env[`${prefix}_FB_ROLE`],
        lowercase_keys: false,
        pageSize: 4096
    };

    if (!config.host || !config.database) {
        throw new Error(`Configuração Firebird crítica faltando para o cliente ${prefix}. Verifique o .env.`);
    }
    return config;
};

const getSupabaseConfig = (clientPrefix) => {
    const prefix = clientPrefix.toUpperCase();

    const config = {
        url: process.env[`${prefix}_SUPABASE_URL`],
        key: process.env[`${prefix}_SUPABASE_KEY`],
    };

    if (!config.url || !config.key) {
        throw new Error(`Configuração Supabase crítica faltando para o cliente ${prefix}. Verifique o .env.`);
    }

    return config; 
};

module.exports = {
    getMssqlConfig,
    getFirebirdConfig,
    getSupabaseConfig
};