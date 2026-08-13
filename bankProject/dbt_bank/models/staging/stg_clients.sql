select
    client_id,
    upper(client_type) as client_type,
    upper(country_code) as country,
    upper(risk_rating) as risk_level,
    onboarding_date,
    upper(status) as client_status
from {{ ref('clients') }}
where client_id is not null
