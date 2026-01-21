/**
 * @param {number} ms
 */

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

require('dotenv').config();
const { iniciarSincronizacao } = require('./etlClient.js');

const clientKey = process.argv[2];

const { logTaskStatus, CURRENT_EXECUTION_ID } = require('./logger');

if (!clientKey) {
    console.error("ERRO: É obrigatório fornecer o nome do cliente.");
    console.log("Uso: node sincronizar.js [nome_do_cliente]");
    console.log("Exemplo: node sincronizar.js aramebras");
    process.exit(1); 
}

const clientPrefix = clientKey.toUpperCase();
console.log(`--- INICIANDO PROCESSAMENTO PARA O CLIENTE: [${clientPrefix}] ---`);

const queryCustomers = `
    SELECT
    CODIGO AS codigo, 
    MAX(rassoc) AS razao_social, 
    MAX(COALESCE(NULLIF(LTRIM(RTRIM(RESUMO)), ''), 'NA')) AS nome_fantasia, 
    MAX(COALESCE(NULLIF(LTRIM(RTRIM(CGC)), ''), 'NA')) AS cnpj, 
    MAX(INSC) AS inscricao_estadual, 
    MAX(endereco) AS logradouro,
    MAX(ibge_logradouro) AS numero, 
    MAX(COALESCE(NULLIF(LTRIM(RTRIM(complemento)), ''), '')) AS complemento,
    MAX(bairro) AS bairro,
    MAX(cidade) AS cidade,
    MAX(estado) AS estado, 
    MAX(cep) AS cep,
    MAX(COALESCE(NULLIF(LTRIM(RTRIM(TEL1)), ''), '')) AS telefone,
    MAX(COALESCE(NULLIF(LTRIM(RTRIM(email)), ''), '')) AS email, 
    MAX(COALESCE(NULLIF(LTRIM(RTRIM(CONTATO)), ''), '')) AS responsavel_comercial,
    (SELECT TOP 1 cl.DESCRI FROM SEGMENTO cl (nolock) WHERE cl.SEGMEN = c.segmento) AS segmento_industrial,
    (SELECT TOP 1 cl.DESCLASSE FROM CLAS_CLI cl (nolock) WHERE cl.CLASSE = c.classe) AS classe,
    (SELECT TOP 1 isnull(f.resumo,f.rassoc) FROM FORNECED f (nolock) 
    WHERE f.codigo = (SELECT TOP 1 CODIGO_FORNECEDOR FROM CONFIGURACAO_CLIENTE_EMPRESA (nolock) where CODIGO_CLIENTE = c.codigo) AND f.comissionado = 's') AS representante,
    'ativo' AS status
    FROM CLIENTES c (NOLOCK) 
    WHERE STATUS <> 'I' 
    GROUP BY CODIGO, c.segmento, c.classe 
    ORDER BY CODIGO ASC
`;

const queryCustomers_Cleanup = `
    SELECT 
        CODIGO AS codigo
    FROM CLIENTES c (NOLOCK) 
    WHERE STATUS <> 'I'
    GROUP BY CODIGO
    ORDER BY CODIGO ASC
`;

const queryCustomerContacts = `
    SELECT
    CC.R_E_C_N_O_ AS recno_id,
    codigo as codigo_cliente,
    ISNULL(LTRIM(RTRIM(nome)), '') AS nome, 
    LTRIM(RTRIM(funcao)) AS funcao,
    fone,
    email,
    cep,
    municipio,
    uf,
    logradouro,
    numero,
    bairro,
    complemento,
    pais,
    (SELECT TOP 1 C.NOME FROM CARGOS c (nolock) WHERE C.R_E_C_N_O_ = CC.RECNO_CARGO) AS cargo
    FROM CONTATOS_CLIENTE CC (nolock)
    ORDER BY CC.R_E_C_N_O_ ASC
`;

const queryCustomerContacts_Cleanup = `
    SELECT 
        CC.R_E_C_N_O_ AS recno_id
    FROM CONTATOS_CLIENTE CC (nolock)
    ORDER BY CC.R_E_C_N_O_ ASC
`;

const querySuppliers = `
    SELECT 
    CODIGO AS codigo, 
    MAX(RASSOC) AS razao_social, 
    MAX(ISNULL(RESUMO,RASSOC)) AS nome_fantasia,
    MAX(COALESCE(NULLIF(LTRIM(RTRIM(CGC)), ''), 'NA')) AS cnpj,
    MAX(ISNULL(INSC,'')) AS inscricao_estadual, 
    MAX(ISNULL(email,'')) as email, 
    MAX(ISNULL(TEL1,'')) AS telefone,
    MAX(ISNULL(ENDERECO,'')) AS logradouro, 
    MAX(ISNULL(IBGE_LOGRADOURO,'')) AS numero,
    MAX(ISNULL(complemento,'')) as complemento, 
    MAX(ISNULL(bairro,'')) as bairro, 
    MAX(ISNULL(cidade,'')) as cidade, 
    MAX(ISNULL(estado,'')) as estado, 
    MAX(ISNULL(cep,'')) as cep, 
    'ativo' as status,
    MAX(R_E_C_N_O_) AS recno_id
    FROM FORNECED (NOLOCK) 
    WHERE STATUS ='A' 
    GROUP BY CODIGO
    ORDER BY CODIGO ASC
`;
const querySuppliers_Cleanup = `
    SELECT 
        CODIGO AS codigo
    FROM FORNECED (NOLOCK) 
    WHERE STATUS ='A' 
    GROUP BY CODIGO
    ORDER BY CODIGO ASC
`;

const queryProducts = `
    WITH RankedProducts AS (
        SELECT 
            LTRIM(RTRIM(p.NUMPEC)) AS code,
            COALESCE(NULLIF(LTRIM(RTRIM(p.DESPEC)), ''), '(Produto sem nome)') AS name,
            COALESCE(NULLIF(LTRIM(RTRIM(p.DESPEC)), ''), '(Produto sem descrição)') AS description,
            '9b823b9b-04f0-4d66-bf3b-3d3c55938054' AS category,
            'UN' AS unit,
            'Aço' AS material,
            REPLACE(CAST(ISNULL(p.peso, 0) AS VARCHAR(50)), ',', '.') AS weight,
            '0' AS cost, 
            '0' AS price,
            'active' AS status,
            REPLACE(CAST(ISNULL((
                SELECT SUM(ISNULL(M.LEADTIME,1)) 
                FROM PROCESSO P2 (NOLOCK)
                INNER JOIN OPERACAO O2 (NOLOCK) ON O2.RECNO_PROCESSO = P2.R_E_C_N_O_
                INNER JOIN MAQUINA M (NOLOCK) ON O2.MAQUIN = M.NUMMAQ
                WHERE P2.R_E_C_N_O_ = P.R_E_C_N_O_
            ), 1) AS VARCHAR(50)), ',', '.') as leadtime,
            p.R_E_C_N_O_ AS recno_id,
            ROW_NUMBER() OVER (PARTITION BY LTRIM(RTRIM(p.NUMPEC)) ORDER BY p.R_E_C_N_O_ DESC) as rn
        FROM dbo.PROCESSO AS p (NOLOCK)
        WHERE ATIVO ='S'
    )
    SELECT
        code, name, description, category, unit, material, 
        weight, cost, price, status, leadtime, recno_id
    FROM RankedProducts
    WHERE rn = 1
    ORDER BY code ASC
`;

const queryProducts_Cleanup = `
    SELECT 
        LTRIM(RTRIM(p.NUMPEC)) AS code
    FROM dbo.PROCESSO AS p (NOLOCK)
    WHERE ATIVO ='S'
    GROUP BY LTRIM(RTRIM(p.NUMPEC))
    ORDER BY LTRIM(RTRIM(p.NUMPEC)) ASC
`;

const queryStockItems = `
    WITH RankedStockItems AS (
        SELECT 
            COALESCE(NULLIF(LTRIM(RTRIM(CODIGO)), ''), 'NA') AS codigo,
            COALESCE(NULLIF(LTRIM(RTRIM(DESCRI)), ''), '(Sem Nome)') as nome, 
            '9b823b9b-04f0-4d66-bf3b-3d3c55938054' as category, 
            'UN' as unit, 
            REPLACE(CAST(ISNULL(SALDOREAL,0) AS VARCHAR(50)), ',', '.') as quantity,
            0 as min_stock, 
            '70fe3687-bc9d-4fe6-9a23-ce1514ac3660' as location,
            REPLACE(CAST(0 AS VARCHAR(50)), ',', '.') as cost, 
            REPLACE(CAST(ISNULL(VALPAGO,0) AS VARCHAR(50)), ',', '.') as price, 
            'active' as status,
            R_E_C_N_O_ AS recno_id,
            ROW_NUMBER() OVER (PARTITION BY LTRIM(RTRIM(CODIGO)) ORDER BY R_E_C_N_O_ DESC) as rn
        FROM ESTOQUE (NOLOCK)
        WHERE STATUS ='A' 
    )
    SELECT
        codigo, nome, category, unit, quantity, min_stock, location, cost, price, status, recno_id
    FROM RankedStockItems
    WHERE rn = 1
    ORDER BY codigo ASC
`;

const queryStockItems_Cleanup = `
    WITH RankedStockItems AS (
        SELECT 
            COALESCE(NULLIF(LTRIM(RTRIM(CODIGO)), ''), 'NA') AS codigo,
            R_E_C_N_O_ AS recno_id,
            ROW_NUMBER() OVER (PARTITION BY LTRIM(RTRIM(CODIGO)) ORDER BY R_E_C_N_O_ DESC) as rn
        FROM ESTOQUE (NOLOCK)
        WHERE STATUS ='A' 
    )
    SELECT
        codigo
    FROM RankedStockItems
    WHERE rn = 1
    ORDER BY codigo ASC
`;

const queryBomItems = `
    WITH RankedBomItems AS (
        SELECT
            LTRIM(RTRIM(p.NUMPEC)) AS code, 
            ISNULL(LTRIM(RTRIM(o.NUMITE)),'') AS codigo,
            COALESCE(NULLIF(LTRIM(RTRIM(o.DESCRI)), ''), '(Componente sem nome)') AS componente,
            CASE CONDIC 
                WHEN 'P' THEN 'Componente' 
                WHEN 'M' THEN 'Matéria-prima' 
                WHEN 'S' THEN 'Terceirizado' END AS tipo,
            ISNULL(o.EXECUT,0) AS quantidade,
            CASE CONDIC 
                WHEN 'P' THEN 'Peça' 
                WHEN 'M' THEN 'Kg' 
                WHEN 'S' THEN 'Peça' END AS unidade,
            0 AS custo_unitario,
            0 AS custo_total,
            o.R_E_C_N_O_ AS recno_id,
            ROW_NUMBER() OVER (
                PARTITION BY LTRIM(RTRIM(p.NUMPEC)), ISNULL(LTRIM(RTRIM(o.NUMITE)),'')
                ORDER BY o.R_E_C_N_O_ DESC
            ) AS rn
        FROM dbo.PROCESSO p (NOLOCK)
        INNER JOIN OPERACAO o (NOLOCK) ON P.R_E_C_N_O_ = o.RECNO_PROCESSO
        WHERE 
            p.ATIVO = 'S' 
            AND o.CONDIC IN ('M','P','S')
            AND p.DESPEC IS NOT NULL 
            AND LTRIM(RTRIM(p.DESPEC)) <> ''
            AND ISNULL(o.EXECUT,0) > 0
    )
    SELECT
        code, codigo, componente, tipo, quantidade, unidade, custo_unitario, custo_total, recno_id
    FROM RankedBomItems
    WHERE rn = 1
    ORDER BY code ASC, codigo ASC;
`;

const queryBomItems_Cleanup = `
    WITH RankedBomItems AS (
        SELECT
            LTRIM(RTRIM(p.NUMPEC)) AS code, 
            ISNULL(LTRIM(RTRIM(o.NUMITE)),'') AS codigo,
            o.R_E_C_N_O_ AS recno_id,
            ROW_NUMBER() OVER (
                PARTITION BY LTRIM(RTRIM(p.NUMPEC)), ISNULL(LTRIM(RTRIM(o.NUMITE)),'')
                ORDER BY o.R_E_C_N_O_ DESC
            ) AS rn
        FROM dbo.PROCESSO p (NOLOCK)
        INNER JOIN OPERACAO o (NOLOCK) ON P.R_E_C_N_O_ = o.RECNO_PROCESSO
        WHERE 
            p.ATIVO = 'S' 
            AND o.CONDIC IN ('M','P','S')
            AND p.DESPEC IS NOT NULL 
            AND LTRIM(RTRIM(p.DESPEC)) <> ''
            AND ISNULL(o.EXECUT,0) > 0
    )
    SELECT
        code, codigo
    FROM RankedBomItems
    WHERE rn = 1
    ORDER BY code ASC, codigo ASC
`;

const queryRoutingOperations = `
    WITH RankedRoutingOps AS (
        SELECT 
            LTRIM(RTRIM(p.NUMPEC)) AS code, 
            CAST(LTRIM(o.NUMOPE) AS INT) AS sequencia,
            COALESCE(NULLIF(LTRIM(RTRIM(o.DESCRI)), ''), '(Operacao sem nome)') AS operacao,
            COALESCE(NULLIF(LTRIM(RTRIM(o.DESCRI)), ''), '(Operacao sem descricao)') AS descricao,
            (CASE WHEN LEN(o.MAQUIN) = 1 THEN 'MAQ00' ELSE 'MAQ0' END + CAST(o.MAQUIN AS VARCHAR(200))) AS maquina,
            REPLACE(CAST(ISNULL(o.EXECUT,0) AS VARCHAR(50)), ',', '.') AS tempo_operacao,
            REPLACE(CAST(CAST(ISNULL(o.REGULA,0) AS DECIMAL(19,2)) / '60' AS VARCHAR(50)), ',', '.') AS tempo_setup,
            '0' AS custo_horario,
            '0' AS custo_total,
            o.R_E_C_N_O_ AS recno_id,
            ROW_NUMBER() OVER (
                PARTITION BY LTRIM(RTRIM(p.NUMPEC)), CAST(LTRIM(o.NUMOPE) AS INT) 
                ORDER BY o.R_E_C_N_O_ DESC
            ) AS rn
        FROM dbo.PROCESSO p (NOLOCK)
        INNER JOIN OPERACAO o (NOLOCK) ON P.R_E_C_N_O_ = o.RECNO_PROCESSO
        WHERE p.ATIVO ='S' AND o.CONDIC IN ('D') 
    )
    SELECT
        code, sequencia, operacao, descricao, maquina, tempo_operacao, tempo_setup, custo_horario, custo_total, recno_id
    FROM RankedRoutingOps
    WHERE rn = 1
    ORDER BY code, sequencia ASC
`;

const queryRoutingOperations_Cleanup = `
    WITH RankedRoutingOps AS (
        SELECT 
            LTRIM(RTRIM(p.NUMPEC)) AS code, 
            CAST(LTRIM(o.NUMOPE) AS INT) AS sequencia,
            o.R_E_C_N_O_ AS recno_id,
            ROW_NUMBER() OVER (
                PARTITION BY LTRIM(RTRIM(p.NUMPEC)), CAST(LTRIM(o.NUMOPE) AS INT) 
                ORDER BY o.R_E_C_N_O_ DESC
            ) AS rn
        FROM dbo.PROCESSO p (NOLOCK)
        INNER JOIN OPERACAO o (NOLOCK) ON P.R_E_C_N_O_ = o.RECNO_PROCESSO
        WHERE p.ATIVO ='S' AND o.CONDIC IN ('D') 
    )
    SELECT
        code, sequencia
    FROM RankedRoutingOps
    WHERE rn = 1
    ORDER BY code ASC, sequencia ASC
`;

const queryProductionOrders = `
    WITH RankedOrders AS (
        SELECT 
            NUMODF as order_number,
            CODPCA as code,
            E.DESCRI AS product_name,
            CAST((ISNULL(QTPEDI,0) - ISNULL(QTDENT,0)) AS INT) AS quantity,
            'pending' AS status,
            'Linha A' AS production_line,
            CAST(dtinicio AS date) as start_date,
            CAST(ISNULL(dtnego, DTENPD) AS date) as delivery_date,
            0 as estimated_hours,
            'Materiais para ' + E.DESCRI as materials,
            'Produzir ' + CAST(CAST((ISNULL(QTPEDI,0) - ISNULL(QTDENT,0)) AS INT) AS VARCHAR(50)) + ' unidades de ' + E.DESCRI AS instructions,
            0 AS timer_elapsed_secounds,
            0 AS timer_paused_seconds,
            'FALSE' as timer_is_running,
            ISNULL((
                SELECT TOP 1 PEDIDO FROM (
                    SELECT PCP.ODFPED AS ODF_PEDIDO, PCP.NUMODF, DTENPD, 
                    (SELECT P1.NUMPED FROM PPEDLISE P1 (NOLOCK) WHERE P1.NUMODF = PCP.ODFPED) AS PEDIDO
                    FROM PPEDLISE PP (NOLOCK)
                    INNER JOIN PCP_ODF_PEDIDO PCP (NOLOCK) ON PP.NUMODF = PCP.ODFPED
                    WHERE PCP.NUMODF IN (SELECT ISNULL(PP1.NUMODF,0) FROM PPEDLISE PP1 (NOLOCK))
                    AND PCP.NUMODF = P.NUMODF
                ) AS TB
            ),'991') AS sales_order,
            P.R_E_C_N_O_ AS recno_id,
            ROW_NUMBER() OVER (
                PARTITION BY NUMODF  
                ORDER BY P.R_E_C_N_O_ DESC 
            ) AS rn
        FROM PPEDLISE P (NOLOCK)
        INNER JOIN ESTOQUE E (NOLOCK) ON E.CODIGO = P.CODPCA 
        WHERE SITUACAO ='991'
    )
    SELECT 
        order_number, code, product_name, quantity, status, production_line, start_date, delivery_date, estimated_hours, materials, instructions, timer_elapsed_secounds, timer_paused_seconds, timer_is_running, sales_order, recno_id
    FROM RankedOrders
    WHERE rn = 1 
    ORDER BY order_number ASC;
`;

const queryProductionOrders_Cleanup = `
    SELECT 
        P.R_E_C_N_O_ AS recno_id
    FROM PPEDLISE P (NOLOCK)
    INNER JOIN ESTOQUE E (NOLOCK) ON E.CODIGO = P.CODPCA 
    WHERE SITUACAO ='991'
    ORDER BY P.R_E_C_N_O_ ASC
`;

const queryPurchaseOrders = `
    select 
    FORNECE AS fornecedor,
    ORIGEM as origem,
    NDOC as pedido,
    ITEM product_code,
    DESCRI as descricao,
    cast(DATAHORA as date) as criado_em,
    isnull(QTPED,0) as qtde_pedido,
    isnull(QTENT,0) as qtde_entregue,
    (ISNULL(QTPED,0) - ISNULL(QTENT,0)) as saldo,
    isnull(VALOR,0) as valor_unitario,
    ((ISNULL(QTPED,0) - ISNULL(QTENT,0)) * isnull(VALOR,0) ) as valor_total,
    cast(DTPED as date) as data_entrega,
    cast(isnull(DTNEGO,DTPED) as date) as data_negociada,
    UNIDADE AS unidade,
    (SELECT TOP 1 G.DESC_GRU FROM GRUPOCPR G (NOLOCK) WHERE G.GRUPO = PARCLISE.GRUPO) as centro_custo,
    ISNULL((
    SELECT top 1 (SELECT PC.DESCRICAO FROM PAG_CONDICAO PC (NOLOCK) WHERE PP.REGRA_PARC = PC.CODIGO) AS REGRA_PARC 
    FROM PEDIDO_FOR PP (NOLOCK) where PP.NDOC = PARCLISE.NDOC
    ),'Não Informado') as cond_pgto,
    ISNULL((
    SELECT top 1 SUM(VALOR) FROM PEDIDOFOR_SINAL_PARC PP (NOLOCK) where PP.NDOC = PARCLISE.NDOC
    ),0) as valor_adiantamento_compra,
    ISNULL((
    SELECT top 1 (SELECT TOP 1 CASE CP.STATUS WHEN 'P' then 'Pago' else 'Aberto' end FROM CONTASP CP (NOLOCK) where CP.R_E_C_N_O_ = PP.TITULO) FROM PEDIDOFOR_SINAL_PARC PP (NOLOCK) where PP.NDOC = PARCLISE.NDOC
    ),'') as status_adiantamento_compra,
    (SELECT top 1 replace(replace(replace(CGC,'.',''),'/',''),'-','') FROM FORNECED f (NOLOCK) WHERE F.CODIGO = PARCLISE.FORNECE) as fornecedor_cnpj,
    R_E_C_N_O_ AS recno_id,
    'em_aberto' as status
    from PARCLISE (NOLOCK)
    ORDER BY NDOC ASC
`;

const queryPurchaseOrders_Cleanup = `
    SELECT 
        R_E_C_N_O_ AS recno_id
    FROM PARCLISE (NOLOCK)
    ORDER BY R_E_C_N_O_ ASC
`;

const queryContasPagar = `
    SELECT
    CODIGO AS fornecedor,
    COALESCE(NULLIF(LTRIM(RTRIM(DOCUMENTO)), ''), '(Sem Documento)') AS numero_documento,
    ISNULL(cast(DATAEMI as date), GETDATE()) AS data_emissao,
    cast(VENCIMENTO as date) AS data_vencimento,
    cast(DATAPAGO as date) AS data_pagamento,
    REPLACE(CAST(ISNULL(valor,0) AS VARCHAR(50)), ',', '.') as valor_bruto,
    REPLACE(CAST(ISNULL(DESCONTO,0) AS VARCHAR(50)), ',', '.') as descontos,
    REPLACE(CAST(ISNULL(juros,0) AS VARCHAR(50)), ',', '.') AS juros,
    REPLACE(CAST(ISNULL(VALORLIQUIDO,0) AS VARCHAR(50)), ',', '.') AS valor_liquido,
    case status
    when 'A' THEN 'em_aberto'
    when 'P' THEN 'pago'
    else
    CASE WHEN status ='A' AND CAST(VENCIMENTO AS date) < CAST(GETDATE() AS date) AND DATAPAGO IS NULL THEN 'atrasado' END
    END AS status,
    (SELECT TOP 1 G.DESC_PLA FROM PLANO G (NOLOCK) WHERE G.PLANO = C.PLANO) AS centro_custo,
    C.R_E_C_N_O_ AS recno_id
    FROM CONTASP C (NOLOCK)
    WHERE STATUS <> 'I'
    AND YEAR(C.DATAEMI) >= 2025
    ORDER BY data_vencimento ASC
`;

const queryContasPagar_Cleanup = `
    SELECT 
        C.R_E_C_N_O_ AS recno_id
    FROM CONTASP C (NOLOCK)
    WHERE STATUS <> 'I'
    AND YEAR(C.DATAEMI) >= 2025
    ORDER BY C.R_E_C_N_O_ ASC
`;

const queryContasReceber = `
    SELECT
    CODIGO AS codigo,
    COALESCE(NULLIF(LTRIM(RTRIM(DOCUMENTO)), ''), '(Sem Documento)') AS documento,
    ISNULL(cast(DATAEMI as date), GETDATE()) AS data_emissao,
    cast(VENCIMENTO as date) AS data_vencimento,
    cast(DATAPAGO as date) AS data_pagamento,
    REPLACE(CAST(ISNULL(valor,0) AS VARCHAR(50)), ',', '.') as valor_bruto,
    REPLACE(CAST(ISNULL(DESCONTO,0) AS VARCHAR(50)), ',', '.') as descontos,
    REPLACE(CAST(ISNULL(juros,0) AS VARCHAR(50)), ',', '.') AS juros,
    REPLACE(CAST(ISNULL(VALORLIQUIDO,0) AS VARCHAR(50)), ',', '.') AS valor_liquido,
    CASE 
        WHEN status = 'R' THEN 'recebido'
        WHEN status = 'A' AND CAST(VENCIMENTO AS date) < CAST(GETDATE() AS date) AND DATAPAGO IS NULL THEN 'atrasado'
        WHEN status = 'A' THEN 'em_aberto'
    END AS status,
    (SELECT TOP 1 G.DESC_GRU FROM GRUPOCPR G (NOLOCK) WHERE G.GRUPO = C.GRUPO) AS centro_custo,
    C.R_E_C_N_O_ AS recno_id
    FROM CONTASR C (NOLOCK)
    WHERE 
        STATUS <> 'I'
        AND C.status IN ('A', 'R')
        AND YEAR(C.DATAEMI) >= 2025
    ORDER BY data_vencimento ASC
`;

const queryContasReceber_Cleanup = `
    SELECT 
        C.R_E_C_N_O_ AS recno_id
    FROM CONTASR C (NOLOCK)
    WHERE 
        STATUS <> 'I'
        AND C.status IN ('A', 'R')
        AND YEAR(C.DATAEMI) >= 2025
    ORDER BY C.R_E_C_N_O_ ASC
`;

const querySalesOrders = `
    SELECT
        P.numodf,
        P.numped AS order_number,
        REPLACE(P.CODPCA,'00000','') AS product_code,
        E.DESCRI AS product_name, 
        ISNULL(CAST(P.DATAHORA AS date), GETDATE()) AS order_date, 
        CAST(P.DTENPD AS date) AS delivery_date, 
        CAST(ISNULL(P.dtnego, P.DTENPD)AS date) AS negotiated_date,
        'pending' AS status, 
        'normal' AS priority,
        ISNULL(P.QTPEDI,0) AS qtd_total_order,
        ISNULL(P.QTDENT,0) AS qtd_total_delivery_order,
        CAST((ISNULL(P.QTPEDI,0) - ISNULL(P.QTDENT,0)) AS INT) AS quantity,
        ISNULL(CAST(ISNULL(P.VALUNI,0) AS decimal(19,6)),0) AS unit_price,
        ISNULL((CAST((ISNULL(P.QTPEDI,0) - ISNULL(P.QTDENT,0)) AS INT) * CAST(ISNULL(P.VALUNI,0) AS decimal(19,6))),0) AS total_price,
        ISNULL((CAST((ISNULL(P.QTPEDI,0) - ISNULL(P.QTDENT,0)) AS INT) * CAST(ISNULL(VALUNI,0) AS decimal(19,6))),0) AS total_value,
        ISNULL((CAST((ISNULL(P.QTPEDI,0) - ISNULL(QTDENT,0)) AS INT) * CAST(ISNULL(VALUNI,0) AS decimal(19,6))),0) AS total_amount,
        C.CGC AS cnpj, 
        P.ORDEMCOMPRA AS ordem_compra,
        COALESCE(T_ENG.ENG_STATUS, 'N') AS eng_concluido,
        P.R_E_C_N_O_ AS recno_id
    FROM PPEDLISE P (NOLOCK) 
    LEFT JOIN ESTOQUE E (NOLOCK) ON E.CODIGO = P.CODPCA 
    LEFT JOIN CLIENTES C (NOLOCK) ON C.CODIGO = P.CLIENTE
    LEFT JOIN (
        SELECT DISTINCT PRO.NUMPEC, 
                CASE WHEN PRO.CONCLUIDO = 'T' THEN 'S' ELSE 'N' END AS ENG_STATUS
        FROM PROCESSO PRO (NOLOCK)
        WHERE PRO.CONCLUIDO = 'T' 
    ) AS T_ENG ON T_ENG.NUMPEC = P.CODPCA
    WHERE P.SITUACAO in ('900')
    ORDER BY P.R_E_C_N_O_ ASC
`;

const querySalesOrders_Cleanup = `
    SELECT P.R_E_C_N_O_ AS recno_id
    FROM PPEDLISE P (NOLOCK) 
    WHERE SITUACAO in ('900')
`;

const querySalesOrdersHistory = `
    WITH UnifiedHistory AS (
        SELECT 
            RTRIM(LTRIM(U.numodf)) AS numodf, 
            U.numped AS order_number, 
            REPLACE(U.CODPCA,'00000','') AS product_code, 
            U.DATAHORA, U.DTENPD, U.dtnego, 
            U.ORDEMCOMPRA, U.CST_DATA_CRIACAO_PEDIDO, 
            ISNULL(U.VALUNI, 0) AS valuni_clean, 
            ISNULL(U.QTPEDI, U.QTDPED) AS qtd_total_order_raw,
            ISNULL(U.QTDENT, 0) AS qtd_total_delivery_order_raw,
            CONVERT(VARCHAR(20), U.CST_DATA_CRIACAO_PEDIDO, 112) + RTRIM(LTRIM(U.numodf)) + REPLACE(U.CODPCA,'00000','') AS recno_id,
            ROW_NUMBER() OVER (
                PARTITION BY U.numodf, REPLACE(U.CODPCA,'00000','')
                ORDER BY U.CST_DATA_CRIACAO_PEDIDO DESC, U.numodf DESC
            ) AS rn
        FROM (
            SELECT 
                numodf, numped, CODPCA, ORDEMCOMPRA, 
                TRY_CAST(DATAHORA AS DATE) AS DATAHORA,
                TRY_CAST(DTENPD AS DATE) AS DTENPD,
                TRY_CAST(dtnego AS DATE) AS dtnego,
                COALESCE(TRY_CAST(QTPEDI AS DECIMAL(19, 6)), 0) AS QTPEDI,  
                COALESCE(TRY_CAST(QTDENT AS DECIMAL(19, 6)), 0) AS QTDENT,  
                COALESCE(TRY_CAST(VALUNI AS DECIMAL(19, 6)), 0) AS VALUNI,  
                P.DATAHORA AS CST_DATA_CRIACAO_PEDIDO,  
                NULL AS QTDPED
            FROM PPEDLISE P (NOLOCK)
            WHERE P.SITUACAO <> '991' 
            UNION ALL 
            SELECT 
                NODF AS numodf, numped, CODPCA, NULL AS ORDEMCOMPRA,
                TRY_CAST(DATAHORA AS DATE) AS DATAHORA,
                TRY_CAST(DTENPD AS DATE) AS DTENPD,
                TRY_CAST(dtnego AS DATE) AS dtnego,
                NULL AS QTPEDI, 
                COALESCE(TRY_CAST(QTDENT AS DECIMAL(19, 6)), 0) AS QTDENT,  
                COALESCE(TRY_CAST(VALUNI AS DECIMAL(19, 6)), 0) AS VALUNI,  
                CST_DATA_CRIACAO_PEDIDO, 
                COALESCE(TRY_CAST(QTDPED AS DECIMAL(19, 6)), 0) AS QTDPED 
            FROM PEDRLISE R (NOLOCK)
            WHERE R.MOTCANC IS NULL AND CST_DATA_CRIACAO_PEDIDO IS NOT NULL
        ) AS U
    )
    SELECT
        recno_id,
        numodf,
        order_number,
        product_code,
        NULL AS product_name, 
        ISNULL(DATAHORA, GETDATE()) AS order_date,
        ISNULL(DTENPD, '1900-01-01') AS delivery_date,
        ISNULL(dtnego, ISNULL(DTENPD, '1900-01-01')) AS negotiated_date,
        'pending' AS status,
        'normal' AS priority,
        qtd_total_order_raw AS qtd_total_order,
        qtd_total_delivery_order_raw AS qtd_total_delivery_order,
        CAST((qtd_total_order_raw - qtd_total_delivery_order_raw) AS INT) AS quantity,
        REPLACE(CAST(valuni_clean AS VARCHAR(50)), ',', '.') AS unit_price,
        REPLACE(CAST((qtd_total_order_raw - qtd_total_delivery_order_raw) * valuni_clean AS VARCHAR(50)), ',', '.') AS total_price,
        REPLACE(CAST((qtd_total_order_raw - qtd_total_delivery_order_raw) * valuni_clean AS VARCHAR(50)), ',', '.') AS total_value,
        REPLACE(CAST(ISNULL((qtd_total_order_raw - qtd_total_delivery_order_raw) * valuni_clean, 0) AS VARCHAR(50)), ',', '.') AS total_amount,
        NULL AS cnpj,
        ORDEMCOMPRA AS ordem_compra,
        NULL AS eng_concluido,
        CST_DATA_CRIACAO_PEDIDO AS dt_criado_em
    FROM UnifiedHistory
    WHERE rn = 1
    ORDER BY numodf ASC
`;

const querySalesOrdersHistory_Cleanup = `
    SELECT numodf, order_number, product_code FROM
    (
        SELECT
            P.numodf, P.numped AS order_number, REPLACE(P.CODPCA,'00000','') AS product_code
        FROM PPEDLISE P (NOLOCK) 
        WHERE SITUACAO <>'991'
        UNION
        SELECT
            NODF AS numodf, numped AS order_number, REPLACE(CODPCA,'00000','') AS product_code
        FROM PEDRLISE R (NOLOCK)
        WHERE R.MOTCANC IS NULL AND CST_DATA_CRIACAO_PEDIDO IS NOT NULL
    ) AS TB
`;

const queryNfRecebimento = `
    WITH XMLNAMESPACES (DEFAULT 'http://www.portalfiscal.inf.br/nfe')
    SELECT 
    notafiscal AS numero_nota,
    fornecedor AS fornecedor_nome,
    cfop,
    ISNULL((SELECT TOP 1 DESCRICAO FROM NATUREZA (NOLOCK) WHERE NATUREZA.NATUREZA = CFOP), '(Sem Natureza)') AS natureza_operacao,
    data_emissao as data_emissao,
    idnota,
    'pendente' AS status
    FROM (
    SELECT       
        CONVERT(VARCHAR, CAST(
            COALESCE(XMLNS.dhEmi, XMLNONS.dhEmi, NX.DATA_EMISSAO) 
        AS DATE), 103) AS DATA_EMISSAO,       
        NX.CFOP,        
        NX.NUMERO AS NOTAFISCAL,        
        NX.RAZAO_SOCIAL AS FORNECEDOR,        
        NX.ID_NOTA AS IDNOTA,
        CASE 
            WHEN DATEDIFF(DAY, ISNULL(COALESCE(XMLNS.dhEmi, XMLNONS.dhEmi), NX.DATAHORA), GETDATE()) <= 2 THEN 'VERDE'
            WHEN DATEDIFF(DAY, ISNULL(COALESCE(XMLNS.dhEmi, XMLNONS.dhEmi), NX.DATAHORA), GETDATE()) <  4 THEN 'AMARELO'
            WHEN DATEDIFF(DAY, ISNULL(COALESCE(XMLNS.dhEmi, XMLNONS.dhEmi), NX.DATAHORA), GETDATE()) >= 4 THEN 'VERMELHO'
            ELSE 'CINZA'
        END AS STATUS,            
        ISNULL(DATEDIFF(DAY, ISNULL(COALESCE(XMLNS.dhEmi, XMLNONS.dhEmi), NX.DATAHORA), GETDATE()), 0) AS TEMPO_PARADO,        
        CASE 
            WHEN ISNULL(DATEDIFF(DAY, ISNULL(COALESCE(XMLNS.dhEmi, XMLNONS.dhEmi), NX.DATAHORA), GETDATE()), 0) <= 5 THEN 'T' 
            ELSE 'F' 
        END AS STATUS_EMISSAO_NF,               
        'F' AS STATUS_COMPRAS  
    FROM NFE_XML_RECEBIMENTO NX (NOLOCK)        
    OUTER APPLY (
        SELECT
            CASE 
                WHEN NX.ARQUIVO IS NOT NULL THEN
                    CAST(CONVERT(VARCHAR(MAX), NX.ARQUIVO) AS XML)
                ELSE NULL
            END AS XmlText
    ) AS RawXml
    OUTER APPLY (
        SELECT TOP 1 N.value('(dhEmi/text())[1]', 'datetime') AS dhEmi
        FROM RawXml.XmlText.nodes('//infNFe/ide') AS T(N)
    ) AS XMLNS
    OUTER APPLY (
        SELECT TOP 1
            COALESCE(
                N.value('(*[local-name()="dhEmi"]/text())[1]', 'datetime'),
                N.value('(*[local-name()="dEmi"]/text())[1]',  'datetime')
            ) AS dhEmi
        FROM RawXml.XmlText.nodes('//*[local-name()="infNFe"]/*[local-name()="ide"]') AS T(N)
    ) AS XMLNONS
    WHERE YEAR(NX.DATAHORA) >= YEAR(GETDATE()) - 1        
        AND NX.NUMERO IS NOT NULL        
        AND NX.STATUS IN (1, 2)         
        AND CONVERT(VARCHAR, CAST(NX.DATAHORA AS DATE), 112) > '20250401'         
        AND NX.ID_NOTA NOT IN (
            SELECT ISNULL(HF.CHV_NFE, 0)
            FROM HISTLISE_FOR HF (NOLOCK)
            WHERE HF.EFETUADOENTR = 'S' 
                AND CONVERT(VARCHAR, CAST(DTEMI AS DATE), 112) > '20250101'
        )
    ) AS TB
    ORDER BY NOTAFISCAL ASC
`;

const queryNfRecebimento_Cleanup = `
    WITH XMLNAMESPACES (DEFAULT 'http://www.portalfiscal.inf.br/nfe')
    SELECT 
        NX.ID_NOTA AS idnota
    FROM NFE_XML_RECEBIMENTO NX (NOLOCK)
    WHERE YEAR(NX.DATAHORA) >= YEAR(GETDATE()) - 1 
        AND NX.NUMERO IS NOT NULL 
        AND NX.STATUS IN (1, 2) 
        AND CONVERT(VARCHAR, CAST(NX.DATAHORA AS DATE), 112) > '20250401'
        AND NX.ID_NOTA NOT IN (
            SELECT ISNULL(HF.CHV_NFE, 0)
            FROM HISTLISE_FOR HF (NOLOCK)
            WHERE HF.EFETUADOENTR = 'S' 
                AND CONVERT(VARCHAR, CAST(DTEMI AS DATE), 112) > '20250101'
        )
    GROUP BY NX.ID_NOTA
    ORDER BY NX.ID_NOTA ASC
`;

const queryFatClienteMensal = `
    SELECT 
    CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
        ISNULL(CAST(ANO AS VARCHAR), '0') + 
        ISNULL(CAST(MES AS VARCHAR), '0') + 
        ISNULL(CAST(DIA AS VARCHAR), '0') + 
        ISNULL(CLI, 'NA')
    ), 2) AS id, 
    ANO, MES, DIA, sum(tb.val) as valor_faturado , cli as cliente, MAX(tb.razao) as razao_cliente
    FROM ( 
        SELECT 
        YEAR(N.EMI) AS ANO, MONTH(N.EMI) AS MES, DAY(N.EMI) AS DIA, N.VALMERC AS VAL, N.CLI, N.RAZAO
        FROM NOTAFISC N (NOLOCK)     
        INNER JOIN CT_OPERACAO O (NOLOCK) ON O.CODIGO = N.CT_OPERACAO      
        WHERE N.SITUACAO ='IMPRESSA' 
        AND O.GERACONTAS ='S' AND N.NOTACLI ='S' AND N.CFOP NOT IN (2101,1201,1949,2949)
        AND N.CLI IS NOT NULL 
        ) AS TB      
    WHERE ANO = YEAR(GETDATE())      
    GROUP BY ANO,MES,DIA,CLI
    ORDER BY ano, mes, dia asc
`;

const mssqlConfig = {
    user: process.env[`${clientPrefix}_MSSQL_USER`],
    password: process.env[`${clientPrefix}_MSSQL_PASSWORD`],
    server: process.env[`${clientPrefix}_MSSQL_SERVER`],
    database: process.env[`${clientPrefix}_MSSQL_DATABASE`],
    options: {
        encrypt: true, 
        trustServerCertificate: true 
    },
    requestTimeout: 300000
};

const supabaseConfig = {
    url: process.env[`SUPABASE_URL_${clientPrefix}`],
    key: process.env[`SUPABASE_KEY_${clientPrefix}`]
};

if (!mssqlConfig.user || !supabaseConfig.url) {
    console.error(`ERRO: Credenciais para o cliente [${clientPrefix}] não encontradas no arquivo .env.`);
    console.log(`Verifique se "SUPABASE_URL_${clientPrefix}" e "${clientPrefix}_MSSQL_USER" existem.`);
    process.exit(1);
}

const listaDeTarefas = [
    {
        nomeEmpresa: `${clientPrefix}-Customers-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryCustomers,
        rpcFunctionName: 'sincronizar_customers',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-Customers-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryCustomers_Cleanup,
        rpcFunctionName: 'cleanup_customers',
        rpcParameterName: 'json_input_codes',
    },


    {
        nomeEmpresa: `${clientPrefix}-CustomerContacts-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryCustomerContacts,
        rpcFunctionName: 'sincronizar_customer_contacts',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-CustomerContacts-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryCustomerContacts_Cleanup,
        rpcFunctionName: 'cleanup_customer_contacts',
        rpcParameterName: 'json_input_codes',
    },


    {
        nomeEmpresa: `${clientPrefix}-Suppliers-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: querySuppliers,
        rpcFunctionName: 'sincronizar_suppliers',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-Suppliers-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: querySuppliers_Cleanup,
        rpcFunctionName: 'cleanup_suppliers',
        rpcParameterName: 'json_input_codes',
    },


    {
        nomeEmpresa: `${clientPrefix}-Products-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryProducts,
        rpcFunctionName: 'sincronizar_products',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-Products-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryProducts_Cleanup, 
        rpcFunctionName: 'cleanup_products', 
        rpcParameterName: 'json_input_codes',
    },


    {
        nomeEmpresa: `${clientPrefix}-StockItems-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryStockItems,
        rpcFunctionName: 'sincronizar_stock_items',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-StockItems-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryStockItems_Cleanup,
        rpcFunctionName: 'cleanup_stock_items',
        rpcParameterName: 'json_input_codes',
    },


    {
        nomeEmpresa: `${clientPrefix}-BomItems-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryBomItems,
        rpcFunctionName: 'sincronizar_bom_items',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-BomItems-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryBomItems_Cleanup,
        rpcFunctionName: 'cleanup_bom_items',
        rpcParameterName: 'json_input_codes',
    },


    {
        nomeEmpresa: `${clientPrefix}-RoutingOps-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryRoutingOperations,
        rpcFunctionName: 'sincronizar_routing_operations',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-RoutingOps-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryRoutingOperations_Cleanup, 
        rpcFunctionName: 'cleanup_routing_operations', 
        rpcParameterName: 'json_input_codes',
    },


    {
        nomeEmpresa: `${clientPrefix}-ProductionOrders-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryProductionOrders,
        rpcFunctionName: 'sincronizar_production_orders',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-ProductionOrders-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryProductionOrders_Cleanup, 
        rpcFunctionName: 'cleanup_production_orders', 
        rpcParameterName: 'json_input_codes',
    },


    {
        nomeEmpresa: `${clientPrefix}-PurchaseOrders-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryPurchaseOrders,
        rpcFunctionName: 'sincronizar_purchase_orders',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-PurchaseOrders-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryPurchaseOrders_Cleanup, 
        rpcFunctionName: 'cleanup_purchase_orders', 
        rpcParameterName: 'json_input_codes',
    },


    {
        nomeEmpresa: `${clientPrefix}-ContasPagar-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryContasPagar,
        rpcFunctionName: 'sincronizar_contas_pagar',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-ContasPagar-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryContasPagar_Cleanup, 
        rpcFunctionName: 'cleanup_contas_pagar', 
        rpcParameterName: 'json_input_codes',
    },


    {
        nomeEmpresa: `${clientPrefix}-ContasReceber-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryContasReceber,
        rpcFunctionName: 'sincronizar_contas_receber',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-ContasReceber-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryContasReceber_Cleanup, 
        rpcFunctionName: 'cleanup_contas_receber', 
        rpcParameterName: 'json_input_codes',
    },


    {
        nomeEmpresa: `${clientPrefix}-SalesOrders-TRUNCATE`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: `SELECT 1 AS placeholder`,
        rpcFunctionName: 'truncate_sales_orders_tables',
        rpcParameterName: 'placeholder',
        usaLotes: false
    },
    {
        nomeEmpresa: `${clientPrefix}-SalesOrders-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: querySalesOrders,
        rpcFunctionName: 'sincronizar_sales_orders',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-SalesOrders-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: querySalesOrders_Cleanup,
        rpcFunctionName: 'cleanup_sales_orders',
        rpcParameterName: 'json_input_codes',
    },


    {
        nomeEmpresa: `${clientPrefix}-SalesOrdersHistory-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: querySalesOrdersHistory,
        rpcFunctionName: 'sincronizar_sales_orders_history',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-SalesOrdersHistory-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: querySalesOrdersHistory_Cleanup,
        rpcFunctionName: 'cleanup_sales_orders_history',
        rpcParameterName: 'json_input_codes',
    },


    {
        nomeEmpresa: `${clientPrefix}-NFRecebimento-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryNfRecebimento,
        rpcFunctionName: 'sincronizar_notas_fiscais_recebimento',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
    {
        nomeEmpresa: `${clientPrefix}-NFRecebimento-Cleanup`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryNfRecebimento_Cleanup,
        rpcFunctionName: 'cleanup_notas_fiscais_recebimento',
        rpcParameterName: 'json_input_codes',
    },


    {
        nomeEmpresa: `${clientPrefix}-FatClienteMensal-Upsert`,
        mssqlConfig: mssqlConfig, supabaseConfig: supabaseConfig,
        query: queryFatClienteMensal,
        rpcFunctionName: 'sincronizar_xdsh_fat_cliente_mensal',
        rpcParameterName: 'json_input',
        usaLotes: true,
    },
];

/**
 * @param {number} ms 
 * @returns {string}
 */
function formatarTempo(ms) {
    if (ms < 0) ms = 0;
    const totalSegundos = Math.floor(ms / 1000);
    
    const horas = Math.floor(totalSegundos / 3600);
    const minutos = Math.floor((totalSegundos % 3600) / 60);
    const segundos = totalSegundos % 60;

    const pad = (num) => String(num).padStart(2, '0');

    return `${pad(horas)}:${pad(minutos)}:${pad(segundos)}`;
}

async function migracaoCompleta() {
    const inicioMigracao = Date.now();
    let totalRegistrosEnviados = 0;
    let totalBytesEnviados = 0;
    let tarefasComFalha = 0;

    const BYTE_TO_MB = 1048576; 

    console.log(`[EXECUTION_ID] Rodada iniciada com ID: ${CURRENT_EXECUTION_ID}`); 

    console.log(`=== INICIANDO MIGRAÇÃO TOTAL [${clientPrefix}] (via RPC) ===`);
    console.log(`Encontradas ${listaDeTarefas.length} tarefas de sincronização (Upsert + Cleanup).`);
    console.log("=================================================");

    for (let i = 0; i < listaDeTarefas.length; i++) {
        const tarefa = listaDeTarefas[i];
        console.log(`\n--- [TAREFA ${i + 1}/${listaDeTarefas.length}] --- INICIANDO: ${tarefa.nomeEmpresa} ---`);
        const inicioTarefa = Date.now();
        let status = 'SUCESSO';
        let mensagemErro = null;
        let registrosDaTarefa = 0;
        let bytesDaTarefa = 0;

        try {
            const resultadoTarefa = await iniciarSincronizacao(tarefa);

            registrosDaTarefa = (resultadoTarefa && resultadoTarefa.totalRegistros) ? resultadoTarefa.totalRegistros : (typeof resultadoTarefa === 'number' ? resultadoTarefa : 0);
            bytesDaTarefa = (resultadoTarefa && resultadoTarefa.totalBytes) ? resultadoTarefa.totalBytes : 0;

            totalRegistrosEnviados += registrosDaTarefa;
            totalBytesEnviados += bytesDaTarefa || 0;

            const fimTarefa = Date.now();
            const tempoTarefaMs = fimTarefa - inicioTarefa;

            const bytesTarefaMB = (bytesDaTarefa / BYTE_TO_MB).toFixed(2);

            console.log(`--- [TAREFA ${i + 1} CONCLUÍDA] (Tempo: ${formatarTempo(tempoTarefaMs)}) ---`);
            console.log(`📦 Tarefa ${i + 1}: registros enviados = ${registrosDaTarefa}${bytesDaTarefa ? ` (${bytesTarefaMB} MB)` : ''}`);

            if (i < listaDeTarefas.length - 1) {
                const isHeavyTask = tarefa.nomeEmpresa.includes('Upsert') && registrosDaTarefa > 100;
                if (isHeavyTask && tempoTarefaMs > 5000) {
                    await sleep(1000);
                }
            }

        } catch (error) {
            tarefasComFalha++;
            status = 'FALHA';
            mensagemErro = error.message;

            const fimTarefa = Date.now();
            const tempoTarefaMs = fimTarefa - inicioTarefa;

            console.error(`!!! ERRO NA TAREFA ${i + 1} (${tarefa.nomeEmpresa}) (Tempo: ${formatarTempo(tempoTarefaMs)}) !!!`);
            console.error(`Detalhe: ${mensagemErro}`);
            console.log(`--- [TAREFA ${i + 1} FALHOU, CONTINUANDO PARA A PRÓXIMA] ---`);
        } finally {
            const fimTarefa = Date.now();

            await logTaskStatus(tarefa, status, inicioTarefa, fimTarefa, mensagemErro, registrosDaTarefa);
        }
    }

    const fimMigracao = Date.now();
    const tempoTotalMs = fimMigracao - inicioMigracao;
    const tempoFormatado = formatarTempo(tempoTotalMs);

    const totalBytesMB = (totalBytesEnviados / BYTE_TO_MB).toFixed(2);

    console.log("=================================================");

    if (tarefasComFalha > 0) {
        console.log(`⚠️ === MIGRAÇÃO CONCLUÍDA COM ${tarefasComFalha} FALHA(S) NÃO CRÍTICA(S)! === ⚠️`);
    } else {
        console.log("🎉 === MIGRAÇÃO CONCLUÍDA COM SUCESSO! === 🎉");
    }

    console.log("=================================================");
    console.log(`📊 Total de Registros Enviados: ${totalRegistrosEnviados}`);
    console.log(`📦 Total de Bytes Enviados: ${totalBytesMB} MB`);
    console.log(`⏱️ Tempo Total: ${tempoFormatado} (HH:MM:SS)`);
    console.log("=================================================");
}

migracaoCompleta();