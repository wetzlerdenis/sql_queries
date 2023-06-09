with basis AS (
SELECT 
    companyid,
    JSON_VALUE(details, "$.invoiceNumber") as invoice_number,
    JSON_VALUE(details, "$.issueDate") as issue_date,
    JSON_VALUE(details, "$.currency") as currency,
    JSON_VALUE(details, "$.subTotal") as subTotal,
    JSON_VALUE(details, "$.amount") as total,
    -- COUNT(lineitems) AS lineitems_count,
FROM osome_public.documents,
UNNEST(JSON_QUERY_ARRAY(details, "$.lineItems")) lineitems
where true
    and DATE(createdat) between DATE("2021-01-01") and CURRENT_DATE()
    and JSON_VALUE(details, "$.invoiceNumber") is not null
    and JSON_VALUE(details, "$.invoiceNumber") != ''
    -- and lower(details ->> 'invoiceNumber') != 'na'
    and JSON_VALUE(details, "$.externalDetails.source") = 'bluesheets'
    and subcategory in ('invoice', 'invoiceOut', 'receipt')
-- GROUP BY 1,2,3,4,5,6 
)
SELECT *,
    count(*) AS number_of_dublicates,
from basis
group by 1,2,3,4,5,6
HAVING COUNT(*) > 1
order by number_of_dublicates desc


with basis AS (
SELECT 
    companyid,
    JSON_VALUE(details, "$.invoiceNumber") as invoice_number,
    JSON_VALUE(details, "$.issueDate") as issue_date,
    JSON_VALUE(details, "$.currency") as currency,
    JSON_VALUE(details, "$.subTotal") as subTotal,
    JSON_VALUE(details, "$.amount") as total,
    subcategory
    -- COUNT(lineitems) AS lineitems_count,
FROM osome_public.documents
--,
-- UNNEST(JSON_QUERY_ARRAY(details, "$.lineItems")) lineitems
where true
    and DATE(createdat) between DATE("2022-01-01") and CURRENT_DATE()
    and JSON_VALUE(details, "$.invoiceNumber") is not null
    and JSON_VALUE(details, "$.invoiceNumber") != ''
    -- and lower(details ->> 'invoiceNumber') != 'na'
    and JSON_VALUE(details, "$.externalDetails.source") = 'bluesheets'
    and subcategory in ('invoice','invoiceIn', 'invoiceOut', 'receipt')
-- GROUP BY 1,2,3,4,5,6 
),
dublicates_calculation as ( 
SELECT *,
    count(*) AS number_of_dublicates,
from basis
where total NOT IN ("0")
group by 1,2,3,4,5,6,7
HAVING COUNT(*) > 1
order by number_of_dublicates desc
)
select 
    subcategory,
    SUM(dublicates_calculation.number_of_dublicates-1)/count(*)
from dublicates_calculation
group by subcategory
