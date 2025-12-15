{% macro log_transform(column) %}
    case
        when {{ column }} > 0 then ln({{ column }})
        else null
    end
{% endmacro %}

{% macro k_bins_discretize(column, k, strategy='quantile') %}
    {% if strategy == 'quantile' %}
        ntile({{ k }}) over (order by {{ column }})
    {% else %}
        width_bucket(
            {{ column }},
            min({{ column }}) over (),
            max({{ column }}) over (),
            {{ k }}
        )
    {% endif %}
{% endmacro %}

{% macro robust_scale(column, source_relation=None) %}
    ({{ column }} -
        (select percentile_cont(0.5) within group (order by {{ column }}) from {{ source_relation }})) /
    nullif(
        (select percentile_cont(0.75) within group (order by {{ column }}) from {{ source_relation }}) -
        (select percentile_cont(0.25) within group (order by {{ column }}) from {{ source_relation }}),
        0
    )
{% endmacro %}

{% macro max_absolute_scale(column) %}
    {{ column }} / nullif(
        greatest(
            abs(max({{ column }}) over ()),
            abs(min({{ column }}) over ())
        ),
        0
    )
{% endmacro %}

{% macro min_max_scale(column, new_min=0.0, new_max=1.0) %}
    (({{ column }} - min({{ column }}) over ()) /
    nullif(max({{ column }}) over () - min({{ column }}) over (), 0)) *
    ({{ new_max }} - {{ new_min }}) + {{ new_min }}
{% endmacro %}

{% macro numerical_binarize(column, strategy='percentile', cutoff=0.5, source_relation=None) %}
    case
        {% if strategy == 'percentile' %}
        when {{ column }} >= (select percentile_cont({{ cutoff }}) within group (order by {{ column }}) from {{ source_relation }}) then 1
        {% else %}
        when {{ column }} >= {{ cutoff }} then 1
        {% endif %}
        else 0
    end
{% endmacro %}

{% macro standardize(column) %}
    ({{ column }} - avg({{ column }}) over ()) /
    nullif(stddev({{ column }}) over (), 0)
{% endmacro %}

{% macro interact(column_one, column_two, interaction='multiplicative') %}
    {% if interaction == 'multiplicative' %}
        {{ column_one }} * {{ column_two }}
    {% elif interaction == 'additive' %}
        {{ column_one }} + {{ column_two }}
    {% endif %}
{% endmacro %}

{% macro label_encode(column) %}
    dense_rank() over (order by {{ column }}) - 1
{% endmacro %}

{% macro one_hot_encode(column, source_relation=None) %}
    {% set categories_query %}
        select distinct {{ column }} as category
        from {{ source_relation }}
        order by {{ column }}
    {% endset %}

    {% set results = run_query(categories_query) %}

    {% if execute %}
        {% for row in results %}
            case when {{ column }} = '{{ row.category }}' then 1 else 0 end as {{ column }}_{{ row.category | replace('-', '_') | replace(' ', '_') | lower }}
            {%- if not loop.last %},{% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}