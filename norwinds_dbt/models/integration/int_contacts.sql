{% set sources = ["stg_hubspot_contacts", "stg_rds_customers"] %}

with merged_customers as (
    {%for source in sources%}
        select {{'contact_id' if 'hubspot' in source else 'null'}} as hubspot_contact_id,
        {{'customer_id' if 'rds' in source else 'null'}} as rds_contact_id,
        first_name, last_name, phone,
        {{'company_id' if 'hubspot' in source else 'null'}} as hubspot_company_id,
        {{'company_id' if 'rds' in source else 'null'}} as rds_company_id
        from {{ref(source)}}
        {%if not loop.last%}
            union all
        {%endif%}
    {%endfor%}
),
final as (
    SELECT
    MAX(rds_contact_id) as rds_contact_id, 
    MAX(hubspot_contact_id) as hubspot_contact_id, 
    first_name, 
    last_name, 
    MAX(phone) as phone, 
    MAX(rds_company_id) as rds_company_id, 
    MAX(hubspot_company_id) as hubspot_company_id
    FROM
    merged_customers
    GROUP BY first_name, last_name
)

SELECT {{ dbt_utils.generate_surrogate_key(['first_name', 'last_name', 'phone']) }} AS contact_pk,
hubspot_contact_id, rds_contact_id,
first_name, last_name, phone, hubspot_company_id, rds_company_id FROM final