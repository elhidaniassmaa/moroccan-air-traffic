-- Dimension: Date with Moroccan Calendar Events, School Holidays & Mega Events
with date_spine as (
    select
        cast(range as date) as date_day
    from range(date '2024-01-01', date '2030-12-31', interval '1 day')
),

events as (
    select * from {{ ref('calendar_events') }}
),

expanded as (
    select
        d.date_day,
        extract(year from d.date_day) as year_num,
        extract(month from d.date_day) as month_num,
        extract(day from d.date_day) as day_num,
        dayname(d.date_day) as day_name,
        case when extract(dayofweek from d.date_day) in (0, 6) then true else false end as is_weekend,
        e.event_name,
        e.event_type
    from date_spine d
    left join events e
      on d.date_day between e.start_date and e.end_date
),

aggregated as (
    select
        date_day,
        year_num,
        month_num,
        day_num,
        day_name,
        is_weekend,
        bool_or(case when event_type = 'religious' then true else false end) as is_religious_holiday,
        bool_or(case when event_type = 'school_holiday' then true else false end) as is_school_holiday,
        bool_or(case when event_type = 'national_holiday' then true else false end) as is_national_holiday,
        bool_or(case when event_type = 'mega_event' then true else false end) as is_mega_event,
        string_agg(event_name, ', ') as active_events
    from expanded
    group by
        date_day,
        year_num,
        month_num,
        day_num,
        day_name,
        is_weekend
)

select
    date_day,
    year_num,
    month_num,
    day_num,
    day_name,
    is_weekend,
    coalesce(is_religious_holiday, false) as is_religious_holiday,
    coalesce(is_school_holiday, false) as is_school_holiday,
    coalesce(is_national_holiday, false) as is_national_holiday,
    coalesce(is_mega_event, false) as is_mega_event,
    active_events
from aggregated
