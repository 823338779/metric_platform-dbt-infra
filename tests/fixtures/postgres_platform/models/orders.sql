select 1 as order_id, date '2024-01-01' as ordered_at, {{ local_pkg.amount('10.25') }} as revenue, 'A' as region
union all
select 2 as order_id, date '2024-02-01' as ordered_at, 20.75::numeric as revenue, 'B' as region
