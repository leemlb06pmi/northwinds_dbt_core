-- {% set colors = ['red', 'blue', 'purple'] %}

-- {% for color in colors %}
--   select '{{ color }}' as number 
--   {% if not loop.last %} union all {% endif %} 
-- {% endfor %}

