WITH lowered_categories as (
    SELECT category_id, lower(category_name) AS category_name, description FROM categories
)

SELECT * FROM lowered_categories