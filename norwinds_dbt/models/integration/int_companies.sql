-- WITH hubspot_companies AS (
--     SELECT * FROM {{ ref('stg_hubspot_companies') }}
-- ),
-- rds_companies AS (
--     SELECT * FROM {{ ref('stg_rds_companies') }}
-- ),
-- merged_companies AS (
--     SELECT
--     h.company_id AS hubspot_company_id,
--     r.company_id as rds_company_id,
--     h.name
--     FROM hubspot_companies h
--     INNER JOIN rds_companies r
--         ON r.name = h.name
-- ),
-- final AS (
--     SELECT
--     MAX(m.hubspot_company_id) as hubspot_company_id,
--     MAX(m.rds_company_id) as rds_company_id,
--     m.name,
--     MAX(r.address) as address, 
--     MAX(r.city) as city, 
--     MAX(r.postal_code) as postal_code, 
--     MAX(r.country) as country
--     FROM merged_companies m
--     LEFT JOIN rds_companies r
--         ON r.name = m.name
--     GROUP BY m.name
-- )

-- SELECT {{dbt_utils.generate_surrogate_key(['name'])}} AS company_pk, hubspot_company_id, rds_company_id,
--     name, address, city, postal_code, country
-- FROM final

{% set sources = ["stg_hubspot_companies", "stg_rds_companies"] %}

with merged_companies as (
    {%for source in sources%}
        select name, {{'company_id' if 'hubspot' in source else 'null'}} as hubspot_company_id,
        {{'company_id' if 'rds' in source else 'null'}} as rds_company_id
        from {{ref(source)}}
        {%if not loop.last%}
            union all
        {%endif%}
    {%endfor%}
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
    LEFT JOIN {{ref('stg_rds_companies')}} r
        ON r.name = m.name
    GROUP BY m.name
)

select * from final