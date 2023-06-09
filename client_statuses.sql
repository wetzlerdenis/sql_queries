WITH pdks AS (
    SELECT DISTINCT process_definition_key
    FROM `healthy-clock-304411.analytics.base_gsheets__sku_for_tickets`  
),
tickets__last_unresolved AS (
    SELECT 
        company_id,
        ARRAY_AGG(ticket_id ORDER BY due_at DESC)[offset(0)] AS ticket_id,
        ARRAY_AGG(name ORDER BY due_at DESC)[offset(0)] AS ticket_name,
        DATE(MAX(due_at)) AS due_date,
    FROM `healthy-clock-304411.analytics.mart_tickets`
    WHERE TRUE
    AND process_definition_key IN (
        SELECT * 
        FROM pdks
        )
    AND status <> "resolved"
    GROUP BY company_id
    ORDER BY company_id

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
        FROM analytics.mart__contract_items__atomized
),
subs AS (
        SELECT 
            subscription_id,
            is_auto_renew,
            churn_reason,
        FROM analytics.base_billy__subscriptions
    ),

contracts AS (
    SELECT *
    FROM analytics.mart__contracts
),

products AS (
    SELECT 
        product_id,
        name,
        service_group,
        main_bucket,
    FROM analytics.ops__products
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
),

one_off_accounting__last AS (
    SELECT 
        company_id,
        ARRAY_AGG(products.name ORDER BY start_date DESC)[offset(0)] AS product_name,
    FROM contracts
    LEFT JOIN contract_items USING(contract_id)
    LEFT JOIN products USING(product_id)
    WHERE TRUE
    AND contracts.status = 'active'
    AND products.service_group = 'accounting'
    AND contract_items.price > 0
    AND duration_type = 'one-time'
    GROUP BY 1
),

subscriptions__last_inactive AS (
    SELECT
        company_id,
        ARRAY_AGG(subscription_id ORDER BY finish_date DESC)[offset(0)] AS subscription_id,
        ARRAY_AGG(name ORDER BY finish_date DESC)[offset(0)] AS product_name,
        ARRAY_AGG(churn_reason ORDER BY finish_date DESC)[offset(0)] AS churn_reason,
        MAX(finish_date) AS finish_date,
    FROM accounting_subs 
    WHERE TRUE
    AND finish_date < CURRENT_DATE()
    GROUP BY company_id
),

subscriptions__last_active AS (
    SELECT
        company_id,
        ARRAY_AGG(name ORDER BY finish_date DESC)[offset(0)] AS product_name,
        ARRAY_AGG(is_auto_renew ORDER BY finish_date DESC)[offset(0)] AS is_auto_renew,
        MAX(finish_date) AS finish_date,
    FROM accounting_subs 
    WHERE TRUE
    AND finish_date >= CURRENT_DATE()
    GROUP BY company_id
),

marked_companies AS (
    SELECT 
        company_id,
        name,
        
        'https://agent.osome.team/companies/' || company_id AS link,

        subscriptions__last_active.company_id IS NOT NULL AS has_active_subscription,
        subscriptions__last_active.product_name AS subscription__last_active,
        subscriptions__last_active.is_auto_renew,
        subscriptions__last_active.finish_date AS subscription_last_active_finish,

        subscriptions__last_inactive.company_id IS NOT NULL AS has_inactive_subscription,
        subscriptions__last_inactive.product_name AS susbcription__inactive,
        subscriptions__last_inactive.churn_reason AS subscription__inactive_churned_reason,
        subscriptions__last_inactive.finish_date AS subscriptions___inactive_finish,

        (subscriptions__last_active.product_name IS NULL
            AND subscriptions__last_inactive.product_name IS NOT NULL
            AND subscriptions__last_inactive.churn_reason IS NULL)
            AS has_only_inactive_subscription_with_empty_churn,

        subscriptions__last_active.company_id IS NULL
            AND subscriptions__last_inactive.company_id IS NULL 
            AS never_had_subscription,

        one_off_accounting__last.product_name AS one_off_product,        
        
        (subscriptions__last_active.company_id IS NOT NULL
            AND subscriptions__last_active.is_auto_renew)
            AS future_recurring_revenue,

        (tickets__last_unresolved.company_id IS NOT NULL
        OR 
            (tickets__last_unresolved.company_id IS NULL
            AND subscriptions__last_active.company_id IS NOT NULL 
            AND subscriptions__last_active.finish_date >= CURRENT_DATE())) 
            AS outstanding_work,

        IF(
            tickets__last_unresolved.ticket_name IS NULL
                AND subscriptions__last_active.product_name IS NOT NULL 
                AND subscriptions__last_active.finish_date >= CURRENT_DATE(),
            'Ticket will be created in future',
            tickets__last_unresolved.ticket_name
        ) AS ticket_name,

        tickets__last_unresolved.due_date,

    FROM `healthy-clock-304411.analytics.mart_companies` 
    LEFT JOIN subscriptions__last_active USING(company_id)
    LEFT JOIN subscriptions__last_inactive USING(company_id)
    LEFT JOIN tickets__last_unresolved USING(company_id)
    LEFT JOIN one_off_accounting__last USING(company_id)

    WHERE TRUE
    AND NOT is_test_company
    AND first_invoice_paid_at IS NOT NULL
    AND branch = 'SG'
    AND has_accounting_contract
),


accounting_statuses AS (
SELECT 
    *,

    CASE 
        WHEN future_recurring_revenue AND outstanding_work 
            THEN 'Active'
        WHEN (NOT future_recurring_revenue) AND (NOT outstanding_work) AND (NOT never_had_subscription) AND (NOT has_only_inactive_subscription_with_empty_churn)
            THEN 'Lost'
        WHEN (NOT future_recurring_revenue) AND outstanding_work 
            THEN 'Finishing'
        WHEN never_had_subscription OR has_only_inactive_subscription_with_empty_churn OR (future_recurring_revenue AND (NOT outstanding_work)) 
            THEN 'TBR or lead'
        END AS accounting_client_status,
    

FROM marked_companies
WHERE TRUE
)

SELECT *
FROM accounting_statuses
