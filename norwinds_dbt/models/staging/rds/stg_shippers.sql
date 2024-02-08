WITH source AS (
    SELECT * FROM {{ source('rds', 'shippers') }}
)

SELECT shipper_id, company_name FROM source