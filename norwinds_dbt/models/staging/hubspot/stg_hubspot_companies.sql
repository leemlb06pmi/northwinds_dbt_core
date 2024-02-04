WITH source AS (
    SELECT * FROM {{ source('hubspot', 'northwinds_hubspot') }}
),
final AS (
    SELECT
    concat('hubspot-', replace(lower(business_name), ' ','-')) AS company_id,
    MAX(business_name) AS name
    FROM source
    GROUP BY business_name
)

SELECT * FROM final