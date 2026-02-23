# ETL Engine - Motor de Integração

Sistema de ETL (Extract, Transform, Load) para sincronização de dados entre bancos Firebird, MSSQL e Supabase.

## 📋 Visão Geral do Projeto

Este projeto é um motor de integração que:

- **Extrai** dados de bancos Firebird (sistema legado)
- **Transforma** e processa os dados conforme necessário
- **Carrega** para bancos MSSQL ou Supabase (destino)

### Arquitetura

```
etl-engine/
├── engines/           # Módulos principais do motor ETL
│   ├── config.js      # Gerenciamento de configurações
│   ├── firebirdClient.js   # Cliente Firebird
│   ├── mssqlClient.js      # Cliente MSSQL
│   ├── disparadorBase.js   # Orquestrador de tarefas
│   ├── sincronizarBase.js  # Base de sincronização
│   └── logger.js           # Sistema de logs
├── clientes/          # Configurações específicas por cliente
│   └── martiaco/      # Exemplo: Cliente Martiaco
│       ├── disparador.js          # Orquestrador do cliente
│       └── sync_*.js             # Scripts de sincronização
└── utils/            # Utilitários e ferramentas de teste
    ├── test_connection.js        # Teste de conexão
    ├── test_firebird.js          # Teste específico Firebird
    └── scanner_firebird.js       # Scanner de estruturas
```

## 🚀 Instalação

### Pré-requisitos

- Node.js (versão 14 ou superior)
- Acesso ao banco Firebird de origem
- Credenciais do banco de destino (MSSQL ou Supabase)

### Passo 1: Instalar Dependências

```bash
npm install
```

### Passo 2: Configurar Variáveis de Ambiente

1. Copie o arquivo de exemplo:

```bash
cp .env.example .env
```

2. Edite o arquivo `.env` com suas credenciais:

```env
# Substitua TESTECLIENT pelo nome do seu cliente (em maiúsculas)
TESTECLIENT_FB_HOST=
TESTECLIENT_FB_PORT=
TESTECLIENT_FB_DATABASE=
TESTECLIENT_FB_USER=
TESTECLIENT_FB_PASSWORD=
TESTECLIENT_FB_ROLE=

# Configure também MSSQL e/ou Supabase se necessário
TESTECLIENT_MSSQL_USER=...
TESTECLIENT_MSSQL_PASSWORD=...
TESTECLIENT_SUPABASE_URL=...
TESTECLIENT_SUPABASE_KEY=...
```

## 🧪 Testando a Conexão

### Teste Básico de Conexão Firebird

Use o script de teste para verificar se a conexão está funcionando:

```bash
node utils/test_connection.js TESTECLIENT
```

Este comando irá:

- ✅ Testar a conexão com o Firebird
- 📊 Verificar acesso às views disponíveis
- 🔍 Listar as colunas da primeira view

### Views Disponíveis

As seguintes views estão disponíveis para consulta:

1. `VW_AXON_CAD_CLIENTE` - Cadastro de clientes
2. `VW_AXON_CAD_ENG_ITEM` - Itens de engenharia
3. `VW_AXON_CAD_ENG_ESTRUTURA` - Estrutura de engenharia
4. `VW_AXON_CAD_ENG_ROTEIRO` - Roteiros de produção
5. `VW_AXON_FATURAMENTO` - Dados de faturamento
6. `VW_AXON_PEDIDO` - Pedidos

### Teste Manual de Query

Para testar uma query específica, você pode criar um script simples:

```javascript
// test_custom_query.js
const path = require("path");
require("dotenv").config({ path: path.join(__dirname, ".env") });
const { executarQueryFirebird } = require("./engines/firebirdClient.js");

const clientKey = "TESTECLIENT";
const sql = "SELECT FIRST 10 * FROM VW_AXON_CAD_CLIENTE";

executarQueryFirebird(clientKey, sql)
  .then((result) => {
    console.log(`✅ ${result.length} registros retornados`);
    console.table(result);
  })
  .catch((err) => {
    console.error("❌ Erro:", err.message);
  });
```

Execute com:

```bash
node test_custom_query.js
```

## 🏗️ Criando um Novo Cliente

### Passo 1: Adicionar Credenciais ao .env

```env
NOVOCLIENTE_FB_HOST=ip_do_servidor
NOVOCLIENTE_FB_PORT=3061
NOVOCLIENTE_FB_DATABASE=/caminho/banco.fdb
NOVOCLIENTE_FB_USER=usuario
NOVOCLIENTE_FB_PASSWORD=senha
NOVOCLIENTE_FB_ROLE=role_name

NOVOCLIENTE_SUPABASE_URL=https://seu-projeto.supabase.co
NOVOCLIENTE_SUPABASE_KEY=sua_chave
```

### Passo 2: Criar Estrutura de Diretórios

```bash
mkdir -p clientes/novocliente
```

### Passo 3: Criar Disparador

```javascript
// clientes/novocliente/disparador.js
const { dispararTodasAsTarefas } = require("../../engines/disparadorBase.js");

const clientKey = "NOVOCLIENTE";

const modules = [
  "sync_customers.js",
  // adicione outros módulos de sincronização aqui
];

dispararTodasAsTarefas(clientKey, modules, __dirname);
```

### Passo 4: Criar Scripts de Sincronização

Use os exemplos em `clientes/martiaco/` como referência para criar seus próprios scripts de sincronização.

## 📊 Como Funciona

### 1. Extração (Firebird)

O `firebirdClient.js` conecta ao banco Firebird e executa queries SQL:

```javascript
const { executarQueryFirebird } = require("./engines/firebirdClient.js");

const dados = await executarQueryFirebird("CLIENTE", "SELECT * FROM TABELA");
```

### 2. Transformação

Cada script `sync_*.js` define a query e as transformações necessárias.

### 3. Carregamento (Destino)

Os dados transformados são enviados para:

- **Supabase**: via RPC functions
- **MSSQL**: via inserções diretas

### 4. Orquestração

O `disparadorBase.js` executa todos os módulos em paralelo:

```javascript
const { dispararTodasAsTarefas } = require("./engines/disparadorBase.js");

dispararTodasAsTarefas("CLIENTE", ["sync_customers.js"], __dirname);
```

## 🔧 Estrutura de um Script de Sincronização

```javascript
const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "../../.env") });

const { iniciarSincronizacao } = require("../../engines/mssqlClient.js");
const {
  getMssqlConfig,
  getSupabaseConfig,
} = require("../../engines/config.js");

const clientKey = process.argv[2];
const clientPrefix = clientKey.toUpperCase();

// Definir a query
const query = `SELECT * FROM VW_AXON_CAD_CLIENTE`;

// Definir tarefas
const listaDeTarefas = [
  {
    nomeEmpresa: `${clientPrefix}-Customers`,
    mssqlConfig: getMssqlConfig(clientPrefix),
    supabaseConfig: getSupabaseConfig(clientPrefix),
    query: query,
    rpcFunctionName: "sincronizar_customers",
    rpcParameterName: "json_input",
    usaLotes: true,
  },
];

// Executar
async function executar() {
  for (const tarefa of listaDeTarefas) {
    await iniciarSincronizacao(tarefa);
  }
}

executar().catch(console.error);
```

## 📝 Logs e Auditoria

O sistema mantém logs detalhados em `engines/logger.js`:

- Status de cada tarefa
- Tempo de execução
- Erros e avisos
- Quantidade de registros processados

## ⚠️ Troubleshooting

### Erro de Conexão Firebird

```
❌ Falha na conexão Firebird: connect ETIMEDOUT
```

**Soluções:**

- Verifique se o firewall permite conexões na porta 3061
- Confirme o IP do servidor
- Teste com `ping` ou `telnet` para o servidor

### Erro de Permissão

```
❌ Erro na query: no permission for SELECT access to TABLE/VIEW VW_AXON_*
```

**Soluções:**

- Verifique se o usuário tem a ROLE correta
- Confirme as permissões no banco Firebird
- Teste com outro usuário com permissões administrativas

### Variável de Ambiente Não Encontrada

```
❌ Configuração Firebird crítica faltando
```

**Soluções:**

- Verifique se o arquivo `.env` existe
- Confirme que o prefixo do cliente está em MAIÚSCULAS
- Certifique-se que todas as variáveis obrigatórias estão preenchidas

## 🔐 Segurança

- ⚠️ **NUNCA** commite o arquivo `.env` no Git
- O arquivo `.env` já está no `.gitignore`
- Use `.env.example` como template sem dados sensíveis
- Rotacione credenciais periodicamente

## 📦 Dependências

- `node-firebird`: Cliente Firebird para Node.js
- `mssql`: Cliente Microsoft SQL Server
- `@supabase/supabase-js`: Cliente Supabase
- `dotenv`: Gerenciamento de variáveis de ambiente

## 🤝 Contribuindo

1. Crie scripts de sincronização reutilizáveis
2. Documente queries complexas
3. Mantenha os logs informativos
4. Teste antes de executar em produção

## 📄 Licença

ISC

---

**Desenvolvido para integração de sistemas empresariais** 🚀
