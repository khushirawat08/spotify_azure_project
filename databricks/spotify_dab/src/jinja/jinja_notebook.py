# Databricks notebook source
parameters=[
    {
        "table":"spotify_cata.silver.factstream",
        "alias":"factstream",
        "cols":["factstream.stream_id","factstream.listen_duration"]
    },
    {
        "table":"spotify_cata.silver.dimuser",
        "alias":"dimuser",
        "cols":["dimuser.user_id","dimuser.user_name"],
        "condition":"factstream.user_id=dimuser.user_id"
    },
    {
        "table":"spotify_cata.silver.dimtrack",
        "alias":"dimtrack",
        "cols":["dimtrack.track_id","dimtrack.track_name"],
        "condition":"factstream.track_id=dimtrack.track_id"
    }


]

# COMMAND ----------

 pip install jinja2

# COMMAND ----------

from jinja2 import Template

# COMMAND ----------

query_text = """
select
    {% for param in parameters %}
        {% for col in param.cols %}
            {{ col }}{% if not loop.last %},{% endif %}
        {% endfor %}
        {% if not loop.last %},{% endif %}
    {% endfor %}

from
    {{ parameters[0]['table'] }} as {{ parameters[0]['alias'] }}

{% for param in parameters[1:] %}
left join {{ param['table'] }} as {{ param['alias'] }}
on {{ param['condition'] }}
{% endfor %}
"""

# COMMAND ----------

jinja_sql_str= Template(query_text)
query=jinja_sql_str.render(parameters=parameters)
print(query)

# COMMAND ----------

display(spark.sql(query))

# COMMAND ----------

