WITH
financial_years AS (
    SELECT *,
    FROM 
        {{ ref('base_core__financial_years') }}
),

paid_periods AS (
    SELECT *
    FROM 
        {{ ref('stg__contracts__paid_periods_by_ticket_type') }}
),

financial_quarters AS (
    SELECT *,
        CASE
        WHEN quarter_start_date < end_date 
            THEN DATE_SUB(
                    DATE_ADD(
                        quarter_start_date, 
                        INTERVAL 1 QUARTER
                    ),
                    INTERVAL 1 DAY
                ) 
        ELSE end_date
        END
        AS quarter_end_date,

    FROM financial_years,
        UNNEST(
            GENERATE_DATE_ARRAY(
                DATE_TRUNC(
                    start_date,
                    MONTH), 
                end_date, 
                INTERVAL 1 QUARTER
                )
            ) AS quarter_start_date
),

months_joined_paid_periods AS (
    SELECT 
        financial_quarters.fy_id,   
        financial_quarters.company_id,  
        financial_quarters.start_date AS fys,
        financial_quarters.end_date AS fye,
        financial_quarters.quarter_start_date,
        financial_quarters.quarter_end_date, 

        paid_periods.process_definition_key,
        paid_periods.start_date,
        paid_periods.finish_date,
    
            paid_periods.start_date <= financial_quarters.quarter_start_date 
        AND financial_quarters.quarter_end_date <= paid_periods.finish_date 
        AS is_quarter_paid,

    FROM 
        financial_quarters
    LEFT JOIN 
        paid_periods ON financial_quarters.company_id = paid_periods.company_id
    WHERE
        process_definition_key IN (
            'ac-gst-report-internal'
        )
)

SELECT 
    {{ dbt_utils.surrogate_key([
        'fy_id', 
        'company_id',
        'process_definition_key',
        'quarter_start_date',
        ]
    ) }} AS company_year_quarter_service_id,

    fy_id,  
    company_id, 
    fys AS fy_start_date,
    fye AS fy_end_date,
    process_definition_key,
    quarter_start_date,
    quarter_end_date,

    LOGICAL_OR(is_quarter_paid) AS is_quarter_paid,
FROM 
    months_joined_paid_periods
{{ dbt_utils.group_by(n=8) }}
ORDER BY 
    company_id, 
    start_date, 
    quarter_start_date
