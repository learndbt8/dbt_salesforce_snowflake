{{
    config(
        materialized = 'table',
        tags = ['marts', 'dimensions']
    )
}}

with date_spine as (
    select
        dateadd(day, seq4(), '2018-01-01'::date) as date_day
    from table(generator(rowcount => 4748))
),

final as (
    select
        date_day,

        -- Day attributes
        dayofmonth(date_day)                                            as day_of_month,
        dayofyear(date_day)                                             as day_of_year,
        dayofweek(date_day)                                             as day_of_week,         -- 0=Sun..6=Sat
        dayofweekiso(date_day)                                          as day_of_week_iso,     -- 1=Mon..7=Sun
        decode(dayofweekiso(date_day),
            1,'Monday', 2,'Tuesday', 3,'Wednesday',
            4,'Thursday', 5,'Friday', 6,'Saturday', 7,'Sunday')        as day_of_week_name,
        decode(dayofweekiso(date_day),
            1,'Mon', 2,'Tue', 3,'Wed',
            4,'Thu', 5,'Fri', 6,'Sat', 7,'Sun')                        as day_of_week_name_short,
        iff(dayofweekiso(date_day) in (6,7), false, true)               as is_weekday,

        -- Week attributes
        weekiso(date_day)                                               as week_of_year_iso,
        week(date_day)                                                  as week_of_year,
        date_trunc('week', date_day)::date                             as week_start_date,      -- Monday
        dateadd(day, 6, date_trunc('week', date_day))::date            as week_end_date,        -- Sunday

        -- Month attributes
        month(date_day)                                                 as month_of_year,
        monthname(date_day)                                             as month_name,
        left(monthname(date_day), 3)                                    as month_name_short,
        date_trunc('month', date_day)::date                            as month_start_date,
        last_day(date_day, 'month')::date                              as month_end_date,

        -- Quarter attributes
        quarter(date_day)                                               as quarter_of_year,
        'Q' || quarter(date_day)::varchar                               as quarter_name,
        date_trunc('quarter', date_day)::date                          as quarter_start_date,
        dateadd(day, -1, dateadd(month, 3,
            date_trunc('quarter', date_day)))::date                    as quarter_end_date,

        -- Year attributes
        year(date_day)                                                  as year_number,
        date_trunc('year', date_day)::date                             as year_start_date,
        last_day(date_day, 'year')::date                               as year_end_date,

        -- Prior year comparisons
        dateadd(year, -1, date_day)::date                              as prior_year_date_day,
        dateadd(day, -364, date_day)::date                             as prior_year_over_year_date_day,

        -- ISO week prior year
        weekiso(dateadd(day, -364, date_day))                          as prior_year_iso_week_of_year,
        date_trunc('week', dateadd(day, -364, date_day))::date         as prior_year_iso_week_start_date,
        dateadd(day, 6, date_trunc('week',
            dateadd(day, -364, date_day)))::date                       as prior_year_iso_week_end_date,

        -- Prior year month
        date_trunc('month', dateadd(year, -1, date_day))::date         as prior_year_month_start_date,
        last_day(dateadd(year, -1, date_day), 'month')::date           as prior_year_month_end_date,

        -- Surrogate key
        to_char(date_day, 'YYYYMMDD')                                   as date_key

    from date_spine
)

select * from final