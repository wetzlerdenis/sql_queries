WITH financial_years AS (
  SELECT
    fy_id,
    company_id,
    start_date, end_date, remarks,
  FROM `healthy-clock-304411.analytics.base_core__financial_years`
),
contracts AS (
    SELECT *
    FROM analytics.mart__contracts
),
contract_items AS (
    SELECT 
        contract_id,
        contract_item_id,
        DATE(start_at) AS start_date,
        DATE(finish_at) AS finish_date,
        price,
        duration_type,
        product_id,
        subscription_id,
        package_name,
        product_sku,
    FROM analytics.mart__contract_items__atomized
),
subs AS (
        SELECT 
            subscription_id,
        FROM analytics.base_billy__subscriptions
    ),

products AS (
    SELECT 
        product_id,
        name,
        service_group,
        main_bucket,
        branch,
        
    FROM analytics.ops__products
), 
tickets AS (
  SELECT 
    ticket_id,
    name as ticket_name,
    contract_id,
    financial_year_id,
    period_start_at,
    period_end_at
  FROM `healthy-clock-304411.analytics.mart_tickets` 
  WHERE business_line = 'accounting'
  AND process_definition_key = "ac-ufs-report-internal"
  AND process_definition_key NOT IN ("ac-other","ac-follow-up","ac-advise-needed")
  AND payment_status = 'paid'
  AND branch = 'SG'
),
companies AS (
  SELECT 
    company_id,
    name as company_name,
    incorporation_date,
  FROM `healthy-clock-304411.analytics.mart_companies` 
  WHERE TRUE
    AND NOT is_test_company
--    AND first_invoice_paid_at IS NOT NULL
    AND branch = 'SG'
),

accounting_subs AS (
    SELECT *
    FROM contracts
    LEFT JOIN contract_items USING(contract_id)
    LEFT JOIN products ON contract_items.product_id = products.product_id
    LEFT JOIN subs USING(subscription_id)
    WHERE TRUE
    AND contracts.status = 'active'
    AND products.service_group = 'accounting'
    AND contract_items.price > 0
    AND finish_date IS NOT NULL 
    AND products.branch = 'SG'
    AND product_sku NOT IN ('SG-AC-OTHR' ,'0008-AT-FCC month', '0008-AT-FCC year','0009-PH-Y')
    AND duration_type != "monthly"
),

acc_sub_fye_matched AS (
  SELECT 
    accounting_subs.company_id,
    'https://agent.osome.team/companies/' || accounting_subs.company_id AS link,
    company_name,
    
    incorporation_date,
    accounting_subs.start_date as subscription_start,
    accounting_subs.finish_date as subscription_end,
    accounting_subs.name as product_name,
    
    financial_years.start_date as fye_start,
    financial_years.end_date as fye_end,

    accounting_subs.start_date < incorporation_date AS is_started_before_incorp,
    accounting_subs.start_date = financial_years.start_date AS is_start_matched,
    accounting_subs.finish_date = financial_years.end_date AS is_end_matched,

    tickets.*

  FROM accounting_subs
  LEFT JOIN financial_years 
        ON accounting_subs.company_id = financial_years.company_id
        AND accounting_subs.finish_date BETWEEN financial_years.start_date AND financial_years.end_date
  LEFT JOIN companies ON accounting_subs.company_id = companies.company_id
  LEFT JOIN tickets ON financial_years.fy_id = tickets.financial_year_id
)

select *
-- FROM accounting_subs
from acc_sub_fye_matched
WHERE TRUE
-- AND NOT is_end_matched
-- AND ticket_id is null 
-- AND CURRENT_DATE() > subscription_end
-- WHERE TRUE
AND company_id = 356376
-- order by company_id DESC
