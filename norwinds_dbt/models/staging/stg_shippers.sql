WITH source AS (
    SELECT * FROM public.shippers
)

SELECT shipper_id, company_name FROM source