WITH source AS (
    SELECT * FROM {{ source('rds', 'customers') }}
), 
renamed AS (
    SELECT
    concat('rds-', replace(lower(company_name), ' ', '-')) AS company_id,
    company_name AS name, 
    MAX(address) AS address, 
    MAX(city) AS city, 
    MAX(postal_code) AS postal_code,
    MAX(country) AS country
    FROM source
    GROUP BY name
)

SELECT * FROM renamed