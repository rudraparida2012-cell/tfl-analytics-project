select
    status_severity,
    status_severity_description,
    count(*) as status_record_count,
    count(distinct line_id) as distinct_line_count
from {{ ref('fact_line_status') }}
group by
    status_severity,
    status_severity_description
