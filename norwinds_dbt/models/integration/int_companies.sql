WITH hubspot_companies AS (
    SELECT * FROM {{ ref('stg_hubspot_companies') }}
),
rds_companies AS (
    SELECT * FROM {{ ref('stg_rds_companies') }}
),
merged_companies AS (
    SELECT
    h.company_id AS hubspot_company_id,
    r.company_id as rds_company_id,
    h.name
    FROM hubspot_companies h
    INNER JOIN rds_companies r
        ON r.name = h.name
),
final AS (
    SELECT
    MAX(m.hubspot_company_id) as hubspot_company_id,
    MAX(m.rds_company_id) as rds_company_id,
    m.name,
    MAX(r.address) as address, 
    MAX(r.city) as city, 
    MAX(r.postal_code) as postal_code, 
    MAX(r.country) as country
    FROM merged_companies m
    LEFT JOIN rds_companies r
        ON r.name = m.name
    GROUP BY m.name
)

SELECT {{dbt_utils.generate_surrogate_key(['name'])}} AS company_pk, hubspot_company_id, rds_company_id,
    name, address, city, postal_code, country
FROM final