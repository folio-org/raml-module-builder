 lst${facet_index + 1} as(
     SELECT
        jsonb_build_object(
            'type' , '${facet.alias}',
            'facetValues',
            json_build_array(
                jsonb_build_object(
                'value', ${facet.alias},
                'count', count)
            )
        ) AS jsonb,
        count as count
    FROM grouped_by
     where ${facet.alias} is not null
     group by ${facet.alias}, count
     order by count desc
     )
