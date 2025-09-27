{{ config(
    materialized='incremental',
    unique_key=['phone', 'address1']
) }}

WITH cleaned_data AS (
    SELECT
        provider_id,
        company,
        facility_type,
        license_number,
        CASE 
            WHEN license_status = 'Full Permit' THEN 'Active'
            WHEN license_status IS NULL OR license_status = '' THEN NULL
            ELSE license_status
        END AS license_status,
        CASE 
            WHEN license_issued ~ '\d{4}-\d{2}-\d{2}' THEN CAST(REGEXP_EXTRACT(license_issued, '\d{4}-\d{2}-\d{2}') AS DATE)
            WHEN license_issued ~ '\d{1,2}/\d{1,2}/\d{2}$' THEN CAST(strptime(license_issued, '%m/%d/%y') AS DATE)
            WHEN license_issued ~ '\d{1,2}/\d{1,2}/\d{4}' THEN CAST(strptime(license_issued, '%m/%d/%Y') AS DATE)
            ELSE NULL
        END AS license_issued,
        CASE 
            WHEN certificate_expiration_date ~ '\d{1,2}/\d{1,2}/\d{2}$' THEN CAST(strptime(certificate_expiration_date, '%m/%d/%y') AS DATE)
            WHEN certificate_expiration_date ~ '\d{1,2}/\d{1,2}/\d{4}' THEN CAST(strptime(certificate_expiration_date, '%m/%d/%Y') AS DATE)
            ELSE NULL
        END AS certificate_expiration_date,
        address1,
        address2,
        city,
        state,
        zip,
        county,
        phone,
        CASE 
            WHEN email IS NOT NULL AND email ~ '.*@.*\..*' 
            THEN LOWER(TRIM(email))
            ELSE NULL
        END AS email,
        primary_contact_name,
        title,
        CASE 
            WHEN capacity IS NOT NULL AND capacity != '' 
            THEN CAST(capacity AS INTEGER)
            ELSE NULL
        END AS capacity,
        CASE 
            WHEN ages_accepted IS NOT NULL AND ages_accepted != '' THEN ages_accepted
            ELSE CONCAT_WS(',',
                CASE WHEN infant_age = 'Y' THEN 'Infant' END,
                CASE WHEN toddler_age = 'Y' THEN 'Toddler' END,
                CASE WHEN preschool_age = 'Y' THEN 'Preschool' END,
                CASE WHEN school_age = 'Y' THEN 'School-age' END
            )
        END AS ages_served,
        accepts_financial_aid,
        schedule,
        etl_timestamp
    FROM {{ source('raw_source', 'childcare_facilities') }}
    {% if is_incremental() %}
        WHERE etl_timestamp > (SELECT MAX(etl_timestamp) FROM {{ this }})
    {% endif %}
)

SELECT 
    provider_id,
    company,
    facility_type,
    license_number,
    license_status,
    license_issued,
    certificate_expiration_date,
    address1,
    address2,
    city,
    state,
    zip,
    county,
    phone,
    email,
    primary_contact_name,
    title,
    capacity,
    NULLIF(ages_served, '') AS ages_served,
    accepts_financial_aid,
    schedule,
    etl_timestamp
FROM cleaned_data
