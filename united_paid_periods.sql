#Stg_contracts_paid_periods_by_ticket_type

WITH 
sku_list AS (
    SELECT DISTINCT 
        item_code, 
        process_definition_key
    FROM 
        {{ ref('stg_gsheets__sku_for_tickets') }}
),

contracts AS (
    SELECT *,
    FROM 
        {{ ref('base_billy__contracts') }}
    WHERE 
        status = 'active'
),

contract_items AS (
    SELECT 
        contract_id,
        DATE(start_at) AS start_date,
        DATE(finish_at) AS finish_date,
        price,
        product_sku,
    FROM 
        {{ ref('stg_billy__contract_items__atomized') }}
),

contract_items_by_service AS (
    SELECT *
    FROM contracts
    LEFT JOIN 
        contract_items USING(contract_id)
    LEFT JOIN 
        sku_list ON contract_items.product_sku = sku_list.item_code
    WHERE TRUE
        AND contract_items.price > 0
        AND contract_items.finish_date IS NOT NULL
        AND sku_list.process_definition_key IS NOT NULL
),

timeline AS (
    SELECT 
        company_id,
        process_definition_key,
        start_date AS dte,
        1 AS inc
    FROM 
        contract_items_by_service

    UNION ALL
    
    SELECT 
        company_id,
        process_definition_key,
           
        DATE_ADD(
           finish_date, 
           INTERVAL 1 DAY) 
        AS dte,

        (-1) AS inc
    FROM 
        contract_items_by_service
),

timleine_upgraded AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                company_id, 
                process_definition_key,
                dte
            ) 
        AS row_num,
    FROM 
        timeline
),

timeframes AS (
    SELECT 
        company_id,
        dte,
        inc,
        row_num,
        process_definition_key,

        SUM(inc) OVER (
            PARTITION BY 
                company_id, 
                process_definition_key
            ORDER BY 
                dte, 
                row_num
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS concurrent,

    FROM 
        timleine_upgraded
    GROUP BY 
        company_id, 
        dte, 
        inc, 
        row_num, 
        process_definition_key
),

timeframes_marked AS (
    SELECT 
        company_id,
        process_definition_key,
        dte,
        inc,
        concurrent,
        concurrent = 1 AND inc = 1 AS is_start,
        concurrent = 0 AS is_end,
    FROM 
        timeframes
),

timeframes_boundaries AS (
    SELECT 
        *,
        LEAD(dte) OVER (
            PARTITION BY 
                company_id, 
                process_definition_key 
            ORDER BY 
                dte
            ) 
        AS next_dte,

        LAG(dte) OVER (
            PARTITION BY 
                company_id, 
                process_definition_key 
            ORDER BY 
                dte
            ) 
        AS previous_dte,
    FROM 
        timeframes_marked
    WHERE 
        is_start 
    OR  is_end
),

timeframes_with_overlaps AS (
    SELECT 
        *,
        DATE_DIFF(dte, next_dte, DAY) = 0 
        AS is_overlap_with_start,
        
        DATE_DIFF(dte, previous_dte, DAY) = 0 
        AS is_overlap_with_end,

    FROM 
        timeframes_boundaries
),

paid_periods AS (
    SELECT 
        *,
        IF(is_start, dte, null) AS start_date,
        
        DATE_SUB(
            LEAD(dte) OVER (
                PARTITION BY 
                    company_id, 
                    process_definition_key 
                ORDER BY 
                dte
                ), 
                INTERVAL 1 DAY
            ) 
        AS finish_date,

    FROM 
        timeframes_with_overlaps
    WHERE
        NOT is_overlap_with_start 
    AND 
        NOT is_overlap_with_end
    OR    is_overlap_with_start IS NULL
    OR    is_overlap_with_end IS NULL
)

SELECT
    {{ dbt_utils.surrogate_key([
        'company_id', 
        'process_definition_key',
        'start_date',
        ]
    ) }} 
    AS paid_period_id,

    company_id,
    process_definition_key,
    start_date,
    finish_date,
FROM 
    paid_periods
WHERE 
    start_date IS NOT NULL
AND finish_date IS NOT NULL
ORDER BY 
    company_id
