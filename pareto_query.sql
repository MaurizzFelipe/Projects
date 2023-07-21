WITH category_summary AS (
    SELECT
        pr.product_category_name AS categoria_produto,
        COUNT(DISTINCT iord.order_id) AS total_de_pedidos,
        CAST(SUM(iord.price) AS numeric(10, 2)) AS total_sales,
        CAST(SUM(iord.price) / COUNT(DISTINCT iord.order_id) AS numeric(10, 2)) AS sales_per_order,
        CAST(AVG(iord.price) AS numeric(10, 2)) AS average_order_value,
        CAST(SUM(iord.price) / (SELECT SUM(price) FROM bookstore.table_name_order_items) * 100 AS numeric(10, 2)) AS sales_percentage
    FROM
        bookstore.table_name_order_items AS iord
    LEFT JOIN
        bookstore.table_name_products AS pr ON iord.product_id = pr.product_id
    WHERE
        pr.product_category_name IS NOT NULL
    GROUP BY
        pr.product_category_name
    ORDER BY
        total_sales DESC
),
pareto_distribution AS (
    SELECT
        *,
        SUM(total_sales) OVER (ORDER BY total_sales DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sales,
        SUM(total_sales) OVER () AS total_sum
    FROM
        category_summary
)
SELECT
    pd.categoria_produto,
    pd.total_de_pedidos,
    pd.total_sales,
    pd.sales_per_order,
    pd.average_order_value,
    pd.sales_percentage,
    CASE
        WHEN pd.cumulative_sales <= 0.8 * pd.total_sum THEN 'A'
        WHEN pd.cumulative_sales <= 0.95 * pd.total_sum THEN 'B'
        ELSE 'C'
    END AS pareto_category
FROM
    pareto_distribution AS pd
ORDER BY
    pareto_category ASC;
