{%
    set base_variables = [
        'sepal_length',
        'sepal_width',
        'petal_length',
        'petal_width',
    ]
%}

with import_iris as (
    select *
    from {{ ref('stg_iris') }}
)
select
    sepal_length,
    sepal_width,
    petal_length,
    petal_width,
    -- K Bins Discretization
    {% for variable in base_variables %}
        {{ k_bins_discretize(column=variable, k=5, strategy='quantile') }} as {{ variable }}_quantile_bin,
        {{ k_bins_discretize(column=variable, k=5, strategy='uniform') }} as {{ variable }}_uniform_bin,
    {% endfor %}
    -- Scaling
    {% for variable in base_variables %}
        {{ robust_scale(column=variable, source_relation=ref('stg_iris')) }} as {{ variable }}_robust_scaled,
        {{ max_absolute_scale(column=variable) }} as {{ variable }}_max_absolute_scaled,
        {{ min_max_scale(column=variable, new_min=0.0, new_max=1.0) }} as {{ variable }}_max_min_max_scaled,
    {% endfor %}
    -- Log Transformation
    {% for variable in base_variables %}
        {{ log_transform(column=variable) }} as {{ variable }}_logged,
    {% endfor %}
    -- Binarization
    {% for variable in base_variables %}
        {{ numerical_binarize(column=variable, strategy='percentile', cutoff=0.5, source_relation=ref('stg_iris')) }} as {{ variable }}_binarized,
    {% endfor %}
    -- Standardization
    {% for variable in base_variables %}
        {{ standardize(column=variable) }} as {{ variable }}_standardized,
    {% endfor %}
    -- Interactions
    {% for i in range(base_variables | length) %}
        {% for j in range(i + 1, base_variables | length) %}
            {% set variable_one = base_variables[i] %}
            {% set variable_two = base_variables[j] %}
            {{ interact(column_one=variable_one, column_two=variable_two) }} as {{ variable_one }}_x_{{ variable_two }}_interaction,
            {{ interact(column_one=variable_one, column_two=variable_two, interaction='additive') }} as {{ variable_one }}_plus_{{ variable_two }}_interaction,
        {% endfor %}
    {% endfor %}
    species,
    -- Label Encoding
    {{ label_encode(column='species') }} as species_label_encoded,
    -- One Hot Encoding
    {{ one_hot_encode(column='species', source_relation=ref('stg_iris')) }}
from {{ ref('stg_iris') }}