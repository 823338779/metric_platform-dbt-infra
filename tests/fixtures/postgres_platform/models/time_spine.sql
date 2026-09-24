select day::date as date_day
from generate_series(date '2024-01-01', date '2024-12-31', interval '1 day') as day
