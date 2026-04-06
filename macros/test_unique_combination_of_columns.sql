{% test unique_combination_of_columns(model, combination_of_columns) %}

with validation as (
    select
        {{ combination_of_columns | join(' || \'|\' || ') }} as unique_combo
    from {{ model }}
),

validation_errors as (
    select
        unique_combo,
        count(*) as occurrences
    from validation
    group by unique_combo
    having count(*) > 1
)

select * from validation_errors

{% endtest %}