WITH source AS (
    SELECT * FROM {{ source('hubspot', 'northwinds_hubspot') }}
),
companies AS (
    SELECT * FROM {{ ref('stg_hubspot_companies') }}
),
id_phone AS (
    SELECT
    concat('hubspot-',hubspot_id) AS contact_id,
    first_name, last_name,
    translate(phone,'().- ','') as phone,
    company_id
    FROM source
    JOIN companies
        ON companies.name = source.business_name
),
final AS (
    SELECT
    contact_id, first_name, last_name,
    CASE WHEN LENGTH(phone) = 10 THEN
        '(' || SUBSTRING(phone, 1, 3) || ') ' || 
        SUBSTRING(phone, 4, 3) || '-' ||
        SUBSTRING(phone, 7, 4) 
        ELSE 'N/A' END AS phone,
    company_id
    FROM id_phone
)

SELECT * FROM final