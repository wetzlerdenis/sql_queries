CASE 
  WHEN tags LIKE '%mia%' THEN 'mia' 
  when tags LIKE '%paidStrikeOff%' THEN 'paidStrikeOff'
  WHEN tags LIKE '%ndStrikeOff%' THEN 'ndStrikeOff'
  END AS mia_status,


SELECT * 
FROM 
  `healthy-clock-304411.dbt_snapshots.snapshot_pablo__journal_entries_history__dbt_tmp` 
qualify (
  count(*) over (
    partition by 
      dbt_scd_id
        )
      ) > 1
order by 
  dbt_scd_id asc
  
 dwh 1

WITH account_assistants AS (
         SELECT distinct on (company_key) company_key,
                                          TRUE AS is_ass_assigned
         FROM dim_company_all_positions_v dcapv
         WHERE position_name IN ('Accounts Assistant')
           AND fcurp_status IN ('Acting')
           AND fcur_status = 'active'
         ORDER BY company_key, appointment_date_key DESC
     ),

     accountant AS (
         SELECT distinct on (company_key) company_key,
                                          TRUE AS is_acc_assigned
         FROM dim_company_all_positions_v dcapv
         WHERE position_name IN ('Accountant')
           AND fcurp_status IN ('Acting')
           AND fcur_status = 'active'
         ORDER BY company_key, appointment_date_key DESC
     )

select
       count(distinct ticket_key),
       ticket_status,
       process_definition_name,
       is_kyc_passed,
       is_acc_assigned,
       is_ass_assigned
from dim_tickets t
         left join dim_users u ON u.user_key = t.report_to_user_key
         left join dim_companies c on t.company_key = c.company_key
        left join accountant a on a.company_key = t.company_key
        left join account_assistants ass on ass.company_key = t.company_key
where process_definition_name in (
--                                   'ac-bank-statements-chasing',
                                  'ac-accounting-start'
--                                   'ac-follow-up'
                                 )
  and (report_to_user_key IS NULL OR u.user_email = 'chatbot@osome.com')
  and branch_country_key = 'SG'
  AND c.deleted_date_key ISNULL
  AND c.deal_status NOT IN ('test', 'junk')
  AND ticket_status <> 'deleted'
  and (is_acc_assigned IS NULL OR is_ass_assigned IS NULL)
-- limit 10
group by process_definition_name, ticket_status, is_kyc_passed




with basis AS (
    select c.company_key,
           c.company_name,
           uen,
           deal_status,
           legal_status,
           is_kyc_passed,
           is_wl_client,
           is_wl_partner,
           management_report_service_period,
           service_type,
           start_at,
           finish_at,
           dci.price,
           p.item_code,
           p.item_name,
           app_status,
           is_dormant,
           company_tags
    FROM dim_contracts dc
             LEFT JOIN dim_contract_items dci ON dc.contract_key = dci.contract_key
             LEFT JOIN fact_products p ON dci.product_key = p.item_key
             LEFT JOIN dim_companies c ON c.company_key = dc.company_key
             LEFT JOIN dim_subscriptions ds3 ON dci.contract_item_key = ds3.contract_item_key
    WHERE c.deleted_date_key ISNULL
      AND dc.contract_status = 'active'
      AND c.deal_status NOT IN ('test', 'junk')
      AND p.item_bucket = 'accounting'
      AND c.branch_country_key = 'SG'
),

     all_data AS (
         SELECT company_key                               AS company_key,
                company_name                              AS company_name,
                deal_status                               AS deal_status,
                legal_status                              AS legal_status,
                is_kyc_passed                             AS is_kyc_passed,
                company_tags,
                CASE
                    WHEN is_wl_client THEN 'client'
                    WHEN is_wl_partner THEN 'partner' END AS wl_role,
                NULL                                      AS partner_company_name,
                management_report_service_period          AS mr_service_period,
                uen,
                is_dormant,
                item_code,
                item_name,
                finish_at,
                service_type,
                price
         FROM basis
         WHERE 1 = 1
           AND price > 0

         UNION

         SELECT rwl.company_key                  AS company_key,
                rwl.company_name                 AS company_name,
                deal_status                      AS deal_status,
                legal_status                     AS legal_status,
                is_kyc_passed,
                company_tags,
                wl_role,
                partner_company_name,
                management_report_service_period AS mr_service_period,
                uen,
                c.is_dormant,
                rwl.item_code,
                rwl.item_name,
                rwl.finish_at,
                service_type,
                rwl.price
         FROM report_ops_white_label_dashboard_v rwl
                  LEFT JOIN dim_companies c ON rwl.company_key = c.company_key
                  LEFT JOIN dim_contract_items dci ON rwl.contract_key = dci.contract_key
                  LEFT JOIN dim_subscriptions ds3 ON dci.contract_item_key = ds3.contract_item_key
         WHERE item_main_bucket = 'Accounting'
           AND c.deleted_date_key ISNULL
           AND c.branch_country_key = 'SG'
           AND invoice_status <> 'draft'
           AND (rwl.price > 0 OR rwl.price is NULL)
           AND rwl.company_key NOT IN (
             SELECT DISTINCT dc.company_key
             FROM dim_contracts dc
                      LEFT JOIN dim_contract_items ON dc.contract_key = dim_contract_items.contract_key
                      LEFT JOIN fact_products ON dim_contract_items.product_key = fact_products.item_key
             WHERE dc.contract_status = 'active'
               AND fact_products.item_bucket = 'accounting'
               AND price > 0
         )
     ),

     last_payroll_date AS (
         SELECT DISTINCT company_key    AS payroll_company_key,
                         max(finish_at) AS max_payroll_finish
         FROM all_data
         WHERE service_type = 'payroll'
           AND finish_at NOTNULL
           AND price > 0
         GROUP BY payroll_company_key
     ),

     last_payroll_service AS (
         SELECT DISTINCT payroll_company_key,
                         item_name AS last_payroll_service_name,
                         max_payroll_finish
         FROM all_data d
                  INNER JOIN last_payroll_date lpd ON lpd.payroll_company_key = d.company_key
         WHERE service_type = 'payroll'
           AND finish_at NOTNULL
           AND price > 0
           AND d.finish_at = max_payroll_finish
     ),

     last_acc_date AS (
         SELECT DISTINCT company_key    AS acc_company_key,
                         max(finish_at) AS max_acc_finish
         FROM all_data
         WHERE service_type <> 'payroll'
           AND finish_at NOTNULL
           AND price > 0
         GROUP BY acc_company_key
     ),

     last_acc_service AS (
         SELECT DISTINCT acc_company_key,
                         item_name AS last_acc_service_name,
                         max_acc_finish
         FROM all_data d
                  INNER JOIN last_acc_date lad ON lad.acc_company_key = d.company_key
         WHERE service_type <> 'payroll'
           AND finish_at NOTNULL
           AND price > 0
           AND d.finish_at = max_acc_finish
     ),


     xero_info as (
         SELECT dim_hero_connections.company_key,
                xero_organisation_data::JSON ->> 'salesTaxBasis' AS sales_tax_basis,
                xero_organisation_data::JSON ->> 'taxNumber'     AS gst_tax_number,
                xero_organisation_data::JSON ->> '_class'        AS xero_tier,
                xero_organisation_data::JSON ->> 'baseCurrency'  as functional_currency
         FROM dim_hero_connections
         WHERE dim_hero_connections.status = 'success'
     ),

     xero_sold_companies AS (
         SELECT DISTINCT company_key AS xero_sold_company_key
         FROM all_data
         WHERE item_code IN ('0008-AT-Xero', 'SG-DIGITAL-XERO')
     ),

     tier0 as (
         SELECT DISTINCT company_key AS tier_0_company_key
         FROM all_data d
                  LEFT JOIN xero_sold_companies xsc ON xsc.xero_sold_company_key = d.company_key
         WHERE 1 = 1
           AND (
                 is_dormant = TRUE
                 OR mr_service_period = 'yearly'
                 OR item_code IN ('0008-AT-FS', '0008-AT-FS+Tax', '08-AT-R+T', 'SG-AC-UFS-TAX', '08-AT-R')
             )
           AND xero_sold_company_key ISNULL
     ),


     bank_feed_companies AS (
         SELECT DISTINCT fact_transactions.company_key AS bank_feed_company_key
         FROM fact_transactions
         WHERE fact_transactions.transaction_source::text = 'Bank Feed'::text
           AND fact_transactions.transaction_date <= now()
     ),

     rocking_unicorn_companies AS (
         SELECT DISTINCT company_key AS rocking_unicorn_company_key
         FROM all_data d
         WHERE item_code IN ('SG-AC-UNI',
                             'SG-AC-UNI-G',
                             'SG-AC-EC-UNICORN-M',
                             'SG-AC-UNIC-REV',
                             'SG-AC-EC-UNICORN-Y',
                             'SG-AC-UNI-G-AB',
                             'SG-AC-UNI-AB',
                             'SG-AC-ROCK',
                             'SG-AC-ROCK-G',
                             'SG-AC-EC-ROCK-M',
                             'SG-AC-ROCK-REV',
                             'SG-AC-EC-ROCK-Y',
                             'SG-AC-ROCK-G-AB',
                             'SG-AC-ROCK-AB',
                             'SG-AC-UNIC-REV-HW')
     ),

     fye as (
         select company_id,
                start_date,
                end_date as fye
         from fact_financial_years ffy
         WHERE start_date <= now()
           AND end_date > now()
           AND ffy.item_deleted_at IS NULL
     ),

     tier1 AS (
         SELECT DISTINCT b.company_key AS tier_1_company_key
         FROM all_data b
                  LEFT JOIN xero_info x ON x.company_key = b.company_key
                  LEFT JOIN fye ON fye.company_id = b.company_key
                  LEFT JOIN rocking_unicorn_companies ruc ON ruc.rocking_unicorn_company_key = b.company_key
                  LEFT JOIN bank_feed_companies bfc ON bfc.bank_feed_company_key = b.company_key
         WHERE 1 = 1
           AND x.sales_tax_basis NOT IN ('ACCRUALS', 'CASH')
           AND (fye.start_date < NOW() AND fye.fye > date '2021-11-01')
           AND (NOT 'ecommerce' = ANY (company_tags) OR company_tags IS NULL)
           AND rocking_unicorn_company_key ISNULL
           AND bank_feed_company_key ISNULL
     ),

     tiers_report AS (
         SELECT data.company_key,
                data.company_name,
                data.deal_status,
                data.legal_status,
                data.is_kyc_passed,
                'https://agent.osome.team/companies/' || data.company_key AS company_link,
                data.mr_service_period,
                CASE WHEN tier_0_company_key NOTNULL THEN 'tier 0' END    AS tier_0,
                CASE WHEN tier_1_company_key NOTNULL THEN 'tier 1' END    AS tier_1
         FROM all_data data
                  LEFT JOIN tier0 ON tier0.tier_0_company_key = data.company_key
                  LEFT JOIN tier1 ON tier1.tier_1_company_key = data.company_key
     ),

     account_assistants AS (
         SELECT distinct on (company_key) company_key,
                                          user_name AS acc_assistant_name
         FROM dim_company_all_positions_v dcapv
         WHERE position_name IN ('Accounts Assistant')
           AND fcurp_status IN ('Acting')
           AND fcur_status = 'active'
         ORDER BY company_key, appointment_date_key DESC
     ),

     accountant AS (
         SELECT distinct on (company_key) company_key,
                                          user_name AS accountant_name
         FROM dim_company_all_positions_v dcapv
         WHERE position_name IN ('Accountant')
           AND fcurp_status IN ('Acting')
           AND fcur_status = 'active'
         ORDER BY company_key, appointment_date_key DESC
     )


SELECT DISTINCT ad.company_key,
                ad.company_name,
                ad.deal_status,
                ad.legal_status,
                ad.is_kyc_passed,
                'https://agent.osome.team/companies/' || ad.company_key     AS company_link,
                ad.wl_role,
                accountant.accountant_name,
                account_assistants.acc_assistant_name,
                fye.fye,
                ad.mr_service_period,
--                 dcl.uen                                                       AS uen,
                CASE
                    WHEN sales_tax_basis IN ('ACCRUALS', 'CASH') THEN 1
                    ELSE 0 END                                              AS gst_reg,
                gst_tax_number,
                ad.partner_company_name,
                dcl.partner_company_name,
                COALESCE(dcl.partner_company_name, ad.partner_company_name) AS partner_company_name,
                COALESCE(xero_tier, 'No Account OR No Connection')          AS xero_tier,
                xero_info.functional_currency,
                CASE
                    WHEN xero_tier in ('PREMIUM') THEN TRUE
                    ELSE FALSE END                                          AS xero_multicurrency,
--                 CASE
--                     WHEN ad.company_key = m.company_key THEN TRUE
--                     ELSE FALSE END                                          AS active_multicurrency,
                max_payroll_finish::DATE,
                last_payroll_service_name,
                max_acc_finish::DATE,
                last_acc_service_name,
--                 first_accounting_payment::DATE,
--                 first_payment_date.first_payment_date::DATE,
                GREATEST(max_payroll_finish, max_acc_finish)                AS paid_until_date
--                 future_recurring_revenue,
--                 outstanding_work,
--                 client_status,
--                 lost_reason
FROM all_data ad
         LEFT JOIN accountant ON accountant.company_key = ad.company_key
         LEFT JOIN account_assistants ON account_assistants.company_key = ad.company_key
         LEFT JOIN xero_info ON xero_info.company_key = ad.company_key
         LEFT JOIN dim_companies_lambda dcl ON dcl.company_key = ad.company_key
         LEFT JOIN last_payroll_service lps ON lps.payroll_company_key = ad.company_key
         LEFT JOIN last_acc_service las ON las.acc_company_key = ad.company_key
--                   LEFT JOIN first_payment_date ON first_payment_date.company_key = ad.company_key
         LEFT JOIN fye ON fye.company_id = ad.company_key
--                   LEFT JOIN statuses s ON s.company_key = data.company_key
--                   LEFT JOIN active_multicurrency m ON m.company_key = data.company_key





with data as (
    SELECT rwl.company_key         AS company_key,
           rwl.company_name        AS company_name,
           deal_status             AS deal_status,
           legal_status            AS legal_status,
           wl_role,
           rwl.partner_company_key as partner,
           partner_company_name,
           invoice_number,
           invoice_status,
           invoice_status_xero,
           rwl.item_code,
           rwl.item_name,
           rwl.item_description,
           rwl.item_duration,
           rwl.item_comment
    FROM report_ops_white_label_dashboard_v rwl
             LEFT JOIN dim_companies ON rwl.company_key = dim_companies.company_key
             LEFT JOIN dim_contract_items dci ON rwl.contract_key = dci.contract_key
            LEFT JOIN fact_products fp on dci.item_code = fp.item_code
    WHERE item_main_bucket = 'Accounting'
      AND dim_companies.deleted_date_key ISNULL
      AND dim_companies.branch_country_key = 'SG'
      AND invoice_status <> 'draft'
      AND (rwl.price > 0 OR rwl.price is NULL)
      AND rwl.company_key NOT IN (
        SELECT DISTINCT dc.company_key
        FROM dim_contracts dc
                 LEFT JOIN dim_contract_items ON dc.contract_key = dim_contract_items.contract_key
                 LEFT JOIN fact_products ON dim_contract_items.product_key = fact_products.item_key
        WHERE dc.contract_status = 'active'
          AND fact_products.item_bucket = 'accounting'
          AND price > 0

    )
)
select *
from data
order by partner



with raf_companies AS  (
    select company_key, company_name, raf_status, raf_answers
from dim_companies
where raf_answers notnull
or raf_status notnull
    )
select document_key,
       document_created_date_key,
       document_updated_at,
       uploader_user_key,
       document_type,
       recipients::JSON ->> 'id' AS recipient_id,
       recipients::JSON ->> 'userId' AS recipient_user_id,
       recipients::JSON ->> 'signed' AS is_signed,
       recipients::JSON ->> 'signedAt' AS signed_at,
       recipients::JSON ->> 'status' AS doc_status,
       recipients::JSON ->> 'needSign' AS is_need_sign,
       recipients::JSON ->> 'companyUserId' AS signed_at,
       d.*
from dim_documents d
LEFT JOIN raf_companies ON d.company_key = raf_companies.company_key
where 1=1
  AND document_key = 991956
  and document_category = 'corporate';


with raf_companies AS  (
    select company_key, company_name, raf_status, raf_answers
from dim_companies
where raf_answers notnull
or raf_status notnull
    )
select distinct document_type, document_classification_category,
                document_classification_label,
                document_subcategory
from dim_documents
LEFT JOIN raf_companies ON dim_documents.company_key = raf_companies.company_key
where 1=1
          and document_category = 'corporate'
          AND (raf_answers notnull or raf_status notnull)


    select *
from dim_companies
where
        company_key = 198616
       and
      (raf_answers notnull or raf_status notnull)



select
       CAST(log_created_at AS DATE),
       changes::JSON ->> 'legalStatus' AS legal_status_update,
       user_first_name,
       user_last_name
from status_companies c
         left join fact_audit_logs l ON l.company_key = c.company_key
         left join dim_users u on l.actor_user_key = u.user_key
where event_name = 'apiCompanyUpdate'
  and object_type = 'Company'
  and changes::JSON ->> 'legalStatus' NOTNULL

