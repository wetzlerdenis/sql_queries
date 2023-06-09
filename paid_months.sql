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

financial_months AS (
    SELECT *,
        CASE
        WHEN LAST_DAY(month_start_date, MONTH) < end_date 
            THEN LAST_DAY(month_start_date, MONTH) 
        ELSE end_date
        END
        AS month_end_date,

    FROM financial_years,
        UNNEST(
            GENERATE_DATE_ARRAY(
                DATE_TRUNC(
                    start_date,
                    MONTH), 
                end_date, 
                INTERVAL 1 MONTH
                )
            ) AS month_start_date
),


months_joined_paid_periods AS (
    SELECT 
        financial_months.fy_id, 
        financial_months.company_id,    
        financial_months.start_date AS fys,
        financial_months.end_date AS fye,
        financial_months.month_start_date,
        financial_months.month_end_date, 

        paid_periods.process_definition_key,
        paid_periods.start_date,
        paid_periods.finish_date,
    
            paid_periods.start_date <= financial_months.month_start_date 
        AND financial_months.month_end_date <= paid_periods.finish_date 
        AS is_month_paid,

    FROM 
        financial_months
    LEFT JOIN 
        paid_periods ON financial_months.company_id = paid_periods.company_id
    WHERE
        process_definition_key IN (
            'ac-payslip-report-internal',
            'ac-ir8a-report-internal',
            'ac-personal-tax-report-internal',
            'ac-accounting-compliance-nd-review'
        )
)

SELECT 
    {{ dbt_utils.surrogate_key([
        'fy_id', 
        'company_id',
        'process_definition_key',
        'month_start_date',
        ]
    ) }} AS company_year_month_service_id,

    fy_id,  
    company_id, 
    fys AS fy_start_date,
    fye AS fy_end_date,
    process_definition_key,
    month_start_date,
    month_end_date,

    LOGICAL_OR(is_month_paid) AS is_month_paid,
FROM 
    months_joined_paid_periods
{{ dbt_utils.group_by(n=8) }}
