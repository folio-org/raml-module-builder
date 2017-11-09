 lst${facet_index + 1} as(
     SELECT
        jsonb_build_object(
            'type' , '${facet.alias}',
            'facetValues',
            json_build_array(
                jsonb_build_object(
                'value', ${facet.alias},
                'count', count4facets)
            )
        ) AS jsonb,
        count4facets as count4facets,
        ${idField}
    FROM grouped_by
     where ${facet.alias} is not null
     group by ${facet.alias}, count4facets, ${idField}
     order by count4facets desc
     )
