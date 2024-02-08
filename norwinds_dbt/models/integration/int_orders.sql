WITH source AS (
    SELECT * FROM {{ ref('stg_orders')}}
)

SELECT {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id', 'order_date', 'customer_id']) }} AS order_pk,
order_date, product_id, employee_id, customer_id, quantity, discount, unit_price
FROM source 

