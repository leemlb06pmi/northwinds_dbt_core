{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ref('int_orders')}}
), 
contacts AS (
    SELECT * FROM {{ref('int_contacts')}}
),
final AS (
    SELECT 
    s.order_pk, c.contact_pk, s.order_date, s.product_id, s.employee_id, s.quantity, s.discount, s.unit_price
    FROM source s
    JOIN contacts c
        ON c.rds_contact_id = s.customer_id
        OR c.hubspot_contact_id = s.customer_id
    ORDER BY order_date 
)

SELECT * FROM final