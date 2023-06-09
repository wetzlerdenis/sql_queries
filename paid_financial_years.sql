WITH
financial_years AS (
    SELECT *,
    FROM 
        {{ ref('base_core__financial_years') }}
),

paid_periods AS (
    SELECT *
    FROM 
        {{ ref('stg__contracts__paid_periods_by_ticket_type') }}
),

fy_joined_paid_periods AS (
    SELECT 
        financial_years.fy_id,  
        financial_years.company_id,  
        financial_years.start_date AS fys,
        financial_years.end_date AS fye,

        paid_periods.process_definition_key,
        paid_periods.start_date,
        paid_periods.finish_date,
    
            paid_periods.start_date <= financial_years.start_date 
        AND financial_years.end_date <= paid_periods.finish_date 
        AS is_fy_paid,

    FROM 
        financial_years
    LEFT JOIN 
        paid_periods ON financial_years.company_id = paid_periods.company_id
)

SELECT 
    {{ dbt_utils.surrogate_key([
        'fy_id', 
        'company_id',
        'process_definition_key',
        ]
    ) }} AS company_year_service_id,

    fy_id,  
    company_id,  
    fys AS start_date,
    fye AS end_date,
    process_definition_key,
    
    LOGICAL_OR(is_fy_paid) AS is_fy_paid,
FROM 
    fy_joined_paid_periods
{{ dbt_utils.group_by(n=6) }}
