{{ config(materialized='table') }}

WITH int_contacts AS (
    SELECT * FROM {{ ref('int_contacts') }}
),
int_companies AS (
    SELECT * FROM {{ ref('int_companies') }}
),
joined_contacts AS (
    SELECT con.contact_pk, con.first_name, con.last_name, con.phone, com.company_pk
    FROM int_contacts con
    JOIN int_companies com
        ON con.hubspot_company_id = com.hubspot_company_id 
        OR con.rds_company_id = com.rds_company_id
)

SELECT * FROM joined_contacts