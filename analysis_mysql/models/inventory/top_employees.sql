WITH sales AS (
    SELECT 
        e.employeeNumber,
        e.firstName,
        e.lastName,
        SUM(o.orderNumber) AS total_orders
    FROM 
        {{ ref('employees') }} AS e
    JOIN 
        {{ ref('customers') }} AS c ON e.employeeNumber = c.salesRepEmployeeNumber
    JOIN 
        {{ ref('orders') }} AS o ON c.customerNumber = o.customerNumber
    GROUP BY 
        e.employeeNumber, e.firstName, e.lastName
)

SELECT 
    employeeNumber,
    firstName,
    lastName,
    total_orders
FROM 
    sales
ORDER BY 
    total_orders DESC
LIMIT 5;
