 lst${facet_index + 1} as(
     SELECT
        jsonb_build_object(
            'type' , '${facet.alias}',
            'facetValues',
            json_build_array(
                jsonb_build_object(
                'value', ${facet.alias},
                'count', cnt)
            )
        ) AS jsonb,
       count AS count
    FROM facets
     where ${facet.alias} is not null
     group by ${facet.alias}, cnt, count
     order by cnt desc
     )