WITH source AS (
    SELECT * FROM {{source('rds', 'orders')}}
),
details AS (
    SELECT * FROM {{source('rds','order_details')}}
),
final AS (
    SELECT o.order_id, CONCAT('rds-',o.employee_id) as employee_id, CONCAT('rds-',o.customer_id) as customer_id, CONCAT('rds-',d.product_id) as product_id, o.order_date, d.quantity, d.discount, d.unit_price
    FROM source o 
    LEFT JOIN details d
        ON d.order_id = o.order_id
)

SELECT * FROM final