# 🚀 Quick Start - ETL Engine

## Resumo do Projeto

**Motor ETL** para integração de dados entre:

- **Origem:** Firebird (sistema legado)
- **Destino:** MSSQL ou Supabase

## ⚡ Como Rodar (Passo a Passo)

### 1️⃣ Instalar Dependências

```bash
npm install
```

**Status:** ✅ Já instaladas

- node-firebird@1.1.9
- mssql@12.1.0
- @supabase/supabase-js@2.81.0
- dotenv@17.2.3

### 2️⃣ Configurar Credenciais

O arquivo `.env` já foi criado com suas credenciais:

```env
TESTECLIENT_FB_HOST=200.178.242.2
TESTECLIENT_FB_PORT=3061
TESTECLIENT_FB_DATABASE=/database/esolution.replicacao.fdb
TESTECLIENT_FB_USER=AXON
TESTECLIENT_FB_PASSWORD=Ke163R5g5tq3
TESTECLIENT_FB_ROLE=AXON_ROLE
```

### 3️⃣ Testar Conexão

```bash
# Teste completo (verifica todas as views)
node utils/test_connection.js TESTECLIENT

# Teste de query customizada
node test_custom_query.js TESTECLIENT
```

**Status Atual:** ⚠️ **TIMEOUT** - Servidor não acessível

- Provável causa: Firewall bloqueando porta 3061
- **Solução:** Liberar seu IP no firewall do servidor

### 4️⃣ Verificar Conectividade de Rede

```bash
# Testar se o servidor responde
ping 200.178.242.2

# Testar se a porta está aberta
nc -zv 200.178.242.2 3061
```

---

## 📊 Views Disponíveis

Após a conexão funcionar, você terá acesso a:

1. ✅ `VW_AXON_CAD_CLIENTE` - Clientes
2. ✅ `VW_AXON_CAD_ENG_ITEM` - Produtos/Itens
3. ✅ `VW_AXON_CAD_ENG_ESTRUTURA` - Estrutura de Produtos (BOM)
4. ✅ `VW_AXON_CAD_ENG_ROTEIRO` - Roteiros de Produção
5. ✅ `VW_AXON_FATURAMENTO` - Faturamento
6. ✅ `VW_AXON_PEDIDO` - Pedidos

---

## 🧪 Exemplo de Uso (Após Conexão OK)

### Consulta Simples

```javascript
// Edite test_custom_query.js
const query = `
    SELECT FIRST 10 * 
    FROM VW_AXON_CAD_CLIENTE
`;
```

Execute:

```bash
node test_custom_query.js TESTECLIENT
```

### Consulta com Filtro

```sql
SELECT *
FROM VW_AXON_CAD_CLIENTE
WHERE CODIGO = '000001'
```

### Programaticamente (em seus scripts)

```javascript
const { executarQueryFirebird } = require("./engines/firebirdClient.js");

const dados = await executarQueryFirebird(
  "TESTECLIENT",
  "SELECT FIRST 10 * FROM VW_AXON_CAD_CLIENTE",
);

console.log(dados); // Array de objetos
```

---

## 🔧 Resolver Problema de Conexão

### Opção 1: Liberar IP no Firewall ⭐ Recomendado

1. Descubra seu IP público: https://www.whatismyip.com/
2. Solicite ao administrador do servidor para liberar:
   - **IP:** [seu IP]
   - **Porta:** 3061
   - **Protocolo:** TCP

### Opção 2: Conectar via VPN

Se o acesso é restrito à rede interna:

1. Conecte à VPN da empresa
2. Execute os testes novamente

### Opção 3: Executar de Servidor com Acesso

Clone e execute o projeto em um servidor/VM que já tenha acesso ao Firebird.

---

## 📁 Estrutura do Projeto

```
etl-engine/
├── .env                    # ✅ Credenciais (já criado)
├── test_custom_query.js    # ✅ Script de teste (já criado)
├── README.md               # ✅ Documentação completa
├── TESTE_CONEXAO.md        # ✅ Guia de troubleshooting
│
├── engines/                # Motor principal
│   ├── config.js           # Gerencia configurações do .env
│   ├── firebirdClient.js   # Cliente Firebird
│   ├── mssqlClient.js      # Cliente MSSQL
│   ├── disparadorBase.js   # Orquestrador de tarefas
│   └── logger.js           # Sistema de logs
│
├── utils/                  # Ferramentas úteis
│   ├── test_connection.js  # ✅ Teste completo (já criado)
│   ├── test_firebird.js    # Teste original
│   └── scanner_firebird.js # Scanner de estruturas
│
└── clientes/               # Configurações por cliente
    └── martiaco/           # Exemplo de implementação
        ├── disparador.js           # Orquestrador
        └── sync_*.js               # Scripts de sincronização
```

---

## 📝 Comandos Úteis

```bash
# Testar conexão
node utils/test_connection.js TESTECLIENT

# Query customizada
node test_custom_query.js TESTECLIENT

# Executar sincronização de um cliente (exemplo)
cd clientes/martiaco
node disparador.js MARTIACO

# Executar um módulo específico
node clientes/martiaco/sync_customers.js MARTIACO

# Verificar conectividade
ping 200.178.242.2
nc -zv 200.178.242.2 3061
```

---

## 🎯 Checklist de Validação

### Antes de Executar

- [ ] Dependências instaladas (`npm install`)
- [ ] Arquivo `.env` configurado
- [ ] Conexão de rede OK (`ping`)
- [ ] Porta acessível (`nc -zv`)
- [ ] Teste de conexão bem-sucedido

### Após Conexão Funcionar

- [ ] Todas as 6 views acessíveis
- [ ] Estrutura de dados validada
- [ ] Query de teste retorna dados
- [ ] Performance aceitável

---

## 🆘 Suporte Rápido

| Problema        | Comando                     | Solução                     |
| --------------- | --------------------------- | --------------------------- |
| Timeout         | `ping 200.178.242.2`        | Liberar IP no firewall      |
| Porta fechada   | `nc -zv 200.178.242.2 3061` | Verificar porta no servidor |
| Sem permissão   | -                           | Verificar ROLE do usuário   |
| View não existe | -                           | Confirmar nome da view      |

---

## 📖 Documentação Adicional

- **README.md** - Documentação completa do projeto
- **TESTE_CONEXAO.md** - Troubleshooting detalhado
- **.env.example** - Template de configuração

---

**Última atualização:** 25/01/2026  
**Status do Projeto:** ✅ Configurado e pronto para uso (aguardando liberação de rede)
