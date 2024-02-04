WITH source AS (
    SELECT * FROM {{source('rds', 'customers')}}
),
companies AS (
    SELECT * FROM dev.stg_rds_companies
),
renamed AS (
    SELECT 
    concat('rds-', customer_id) AS customer_id, 
    split_part(contact_name, ' ', 1) AS first_name,
    split_part(contact_name, ' ', 2) AS last_name,
    translate(phone,'().- ','') as updated_phone,
    company_id
    FROM source
    JOIN companies
        ON companies.name = source.company_name
),
final AS (
    SELECT customer_id, first_name, last_name,
    CASE 
        WHEN LENGTH(updated_phone) = 10 THEN
            '(' || SUBSTRING(updated_phone, 1, 3) || ') ' || 
            SUBSTRING(updated_phone, 4, 3) || '-' ||
            SUBSTRING(updated_phone, 7, 4) 
        ELSE 'N/A' END AS phone,
    company_id
    FROM renamed
)
SELECT * FROM final

