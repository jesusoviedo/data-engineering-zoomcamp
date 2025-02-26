
{#
    This macro returns all elements in a given list to lowercase. 
#}

{% macro lower_list(elements) %}
    {% set lower_elements = elements | map('lower') | map("replace", "'", "''") %}
    ({{ lower_elements | map('tojson') | join(', ') }})
{% endmacro %}
