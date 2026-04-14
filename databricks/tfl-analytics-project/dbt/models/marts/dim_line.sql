select distinct
    line_id,
    line_name,
    mode_name
from {{ ref('int_line_status') }}
