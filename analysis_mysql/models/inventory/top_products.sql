WITH sold_products AS (
    SELECT 
        p.productCode,
        p.productName,
        SUM(od.quantityOrdered) AS total_sold
    FROM 
        {{ ref('orderdetails') }} AS od
    JOIN 
        {{ ref('products') }} AS p ON od.productCode = p.productCode
    GROUP BY 
        p.productCode, p.productName
)

SELECT 
    productCode,
    productName,
    total_sold
FROM 
    sold_products
ORDER BY 
    total_sold DESC
LIMIT 5;
