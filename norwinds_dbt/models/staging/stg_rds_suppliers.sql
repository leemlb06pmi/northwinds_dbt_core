WITH source AS (
    SELECT * FROM public.suppliers
), 
renamed AS (
    SELECT supplier_id, company_name, 
    split_part(contact_name, ' ', 1) AS contact_first_name,
    split_part(contact_name, ' ', 2) AS contact_last_name, 
    contact_title, address, city, region, postal_code, country, phone, fax, homepage
    FROM source
),
removed_chars AS (
    SELECT translate(phone,'().- ','') AS phone
    FROM renamed
),

removed_non_standard AS (
    SELECT phone FROM removed_chars
    WHERE Length(phone) = 10
),

parened_spaced_phone AS (
    SELECT format('(%s) %s', LEFT(phone,3), RIGHT(phone,-3)) AS phone
    FROM removed_non_standard 
),

insert_dash AS (
    SELECT format('%s-%s', LEFT(phone,-4), RIGHT(phone,4)) AS phone
    FROM parened_spaced_phone
)




SELECT * FROM insert_dash
