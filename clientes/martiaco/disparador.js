const { dispararTodasAsTarefas } = require('../../engines/disparadorBase.js');

const clientKey = 'MARTIACO';

const modules = [
    'sync_customers.js',
    'sync_customersContacts.js',
    'sync_suppliers.js',
    'sync_products.js',
    'sync_stockItems.js',
    'sync_bomItems.js',
    'sync_routingOperations.js',
    'sync_productionOrders.js',
    'sync_purchaseOrders.js',
    'sync_contasPagar.js',
    'sync_contasReceber.js',
    'sync_salesOrders.js',
    'sync_salesOrdersHistory.js',
    'sync_nfRecebimento.js',
    'sync_fatClienteMensal.js',
    'sync_rhFuncionarios.js',
    'sync_histApontamento.js',
];

dispararTodasAsTarefas(clientKey, modules, __dirname);