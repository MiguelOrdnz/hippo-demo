WITH unique_quantities AS (
    SELECT
        ndc,
        quantity,
        count(distinct id) as occurrences,
    FROM
        main.claims
    group by 
        ndc,
        quantity,
),
sorted_quantities AS (
    SELECT
        ndc,
        array_agg(quantity ORDER BY occurrences DESC) AS most_prescribed_quantity
    FROM
        unique_quantities
    GROUP BY
        ndc
)
SELECT
    ndc,
    array_slice(most_prescribed_quantity, 0, 5) most_prescribed_quantity
FROM
    sorted_quantities