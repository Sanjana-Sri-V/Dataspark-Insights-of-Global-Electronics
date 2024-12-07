use data_spark;
SELECT * from customers_data;
SELECT * from products_data;
SELECT * from sales_data;
SELECT * from stores_data;
SELECT * from exchange_rates;

/** CUSTOMER ANALYSIS **/
/** Gender Distribution **/
SELECT Gender, COUNT(*) AS Count
FROM customers_data
GROUP BY Gender;

/** Age distribution **/
SELECT 
  CASE 
    WHEN TIMESTAMPDIFF(YEAR, Birthday, CURDATE()) BETWEEN 18 AND 24 THEN '18-24'
    WHEN TIMESTAMPDIFF(YEAR, Birthday, CURDATE()) BETWEEN 25 AND 34 THEN '25-34'
    WHEN TIMESTAMPDIFF(YEAR, Birthday, CURDATE()) BETWEEN 35 AND 44 THEN '35-44'
    WHEN TIMESTAMPDIFF(YEAR, Birthday, CURDATE()) BETWEEN 45 AND 54 THEN '45-54'
    WHEN TIMESTAMPDIFF(YEAR, Birthday, CURDATE()) >= 55 THEN '55+'
  END AS Age_Group,
  COUNT(*) AS Count
FROM customers_data
GROUP BY Age_Group;

/** Customer segmentation **/
SELECT 
  CustomerKey,
  COUNT(DISTINCT Order_Number) AS Purchase_Frequency,
  SUM(p.Unit_Price_USD * s.Quantity) AS Total_Spent,
  CASE 
    WHEN COUNT(DISTINCT Order_Number) >= 10 AND SUM(p.Unit_Price_USD * s.Quantity) > 500 THEN 'High Value Frequent Buyer'
    WHEN COUNT(DISTINCT Order_Number) >= 5 AND SUM(p.Unit_Price_USD * s.Quantity) BETWEEN 200 AND 500 THEN 'Medium Value Frequent Buyer'
    WHEN COUNT(DISTINCT Order_Number) < 5 AND SUM(p.Unit_Price_USD * s.Quantity) <= 200 THEN 'Low Value Infrequent Buyer'
  END AS Customer_Segment
FROM sales_data s
JOIN products_data p ON s.ProductKey = p.ProductKey
GROUP BY CustomerKey;

/** Purchase frequency **/
SELECT c.CustomerKey,
       c.Gender,
       AVG(sd.Quantity * p.Unit_Price_USD) AS Avg_Order_Value,
       COUNT(DISTINCT sd.Order_Number) AS Purchase_Frequency,
       GROUP_CONCAT(DISTINCT p.Product_Name) AS Preferred_Products
FROM sales_data sd
JOIN customers_data c ON sd.CustomerKey = c.CustomerKey
JOIN products_data p ON sd.ProductKey = p.ProductKey
GROUP BY c.CustomerKey, c.Gender;

/** customers based on location **/
SELECT City, State, Country, Continent, COUNT(*) AS Count
FROM customers_data
GROUP BY City, State, Country, Continent;

/** PRODUCT ANALYSIS **/
/** Count of total products **/
SELECT 
    COUNT(*) AS Total_Products
FROM 
    products_data;

/** Top 5 products **/
SELECT 
    p.Product_Name,
    p.Brand,
    SUM(s.Quantity) AS Total_Sales_Quantity
FROM 
    sales_data s
JOIN 
    products_data p ON s.ProductKey = p.ProductKey
GROUP BY 
    p.Product_Name, p.Brand
ORDER BY 
    Total_Sales_Quantity DESC
LIMIT 5;

/** Profitability Analysis **/
SELECT 
    p.Product_Name,
    p.Brand,
    p.Unit_Cost_USD,
    p.Unit_Price_USD,
    (p.Unit_Price_USD - p.Unit_Cost_USD) AS Profit_Per_Unit,
    ((p.Unit_Price_USD - p.Unit_Cost_USD) / p.Unit_Cost_USD) * 100 AS Profit_Margin_Percent
FROM 
    products_data p
ORDER BY 
    Profit_Margin_Percent DESC;
    
/** Category analysis **/
SELECT 
    Category AS Product_Category,
    COUNT(Product_Name) AS Total_Products,
    SUM(Unit_Price_USD) AS Total_Sales_Revenue
FROM 
    products_data
GROUP BY 
    Category
ORDER BY 
    Total_Sales_Revenue DESC;

SELECT 
    Subcategory AS Product_Subcategory,
    COUNT(Product_Name) AS Total_Products,
    SUM(Unit_Price_USD) AS Total_Sales_Revenue
FROM 
    products_data
GROUP BY 
    Subcategory
ORDER BY 
    Total_Sales_Revenue DESC;
    
/** Sales analysis **/
/**  Overall Sales Performance **/

SELECT 
    DATE_FORMAT(Order_Date, '%Y-%m') AS Month_Year, 
    SUM(s.Quantity * p.Unit_Price_USD) AS Total_Sales_USD
FROM 
    sales_data s
JOIN 
    products_data p ON s.ProductKey = p.ProductKey
GROUP BY 
    Month_Year
ORDER BY 
    Month_Year;
    
/** Average Sales Per Day **/
SELECT 
    AVG(Daily_Sales) AS Avg_Sales_Per_Day_USD
FROM (
    SELECT 
        Order_Date, 
        SUM(s.Quantity * p.Unit_Price_USD) AS Daily_Sales
    FROM 
        sales_data s
    JOIN 
        products_data p ON s.ProductKey = p.ProductKey
    GROUP BY 
        Order_Date
) AS Daily_Sales_Data;

/** Sales by Store **/
SELECT 
    st.StoreKey, 
    st.Country, 
    st.State, 
    SUM(s.Quantity * p.Unit_Price_USD) AS Store_Total_Sales_USD
FROM 
    sales_data s
JOIN 
    stores_data st ON s.StoreKey = st.StoreKey
JOIN  
    products_data p ON s.ProductKey = p.ProductKey
GROUP BY 
    st.StoreKey, st.Country, st.State
ORDER BY 
    Store_Total_Sales_USD DESC;
    
/** Sales by Product:Top Products by Revenue Generated **/
SELECT 
    p.Brand, 
    SUM(s.Quantity * p.Unit_Price_USD) AS Total_Revenue_USD
FROM 
    sales_data s
JOIN 
    products_data p ON s.ProductKey = p.ProductKey
GROUP BY 
    p.Brand
ORDER BY 
    Total_Revenue_USD DESC
LIMIT 5;

/** Total Sales by Currency **/
SELECT 
    s.Currency_Code, 
    SUM(s.Quantity * p.Unit_Price_USD * e.Exchange) AS Total_Sales_Local_Currency
FROM 
    sales_data s
JOIN 
    products_data p ON s.ProductKey = p.ProductKey
JOIN 
    exchange_rates_data e ON s.Currency_Code = e.Currency_Code
    AND s.Order_Date = e.Date 
GROUP BY 
    s.Currency_Code
ORDER BY 
    Total_Sales_Local_Currency DESC;

/** Store analysis **/
/** Store Performance Analysis **/
SELECT 
    s.StoreKey,
    s.Country,
    s.State,
    s.Square_Meters,
    SUM(sd.Quantity * p.Unit_Price_USD) AS Total_Sales,
    SUM(sd.Quantity * p.Unit_Price_USD) / s.Square_Meters AS Sales_Per_Square_Meter,
    DATEDIFF(CURRENT_DATE, s.Open_Date) / 365 AS Store_Age_Years
FROM 
    sales_data sd
JOIN 
    stores_data s ON sd.StoreKey = s.StoreKey
JOIN 
    products_data p ON sd.ProductKey = p.ProductKey
GROUP BY 
    s.StoreKey, s.Country, s.State, s.Square_Meters, s.Open_Date;

/** Store Contribution to Overall Sales **/
SELECT 
    s.StoreKey,
    s.State,
    s.Country,
    SUM(sd.Quantity * p.Unit_Price_USD) AS Store_Sales,
    (SUM(sd.Quantity * p.Unit_Price_USD) / 
     (SELECT SUM(sd1.Quantity * p1.Unit_Price_USD) 
      FROM sales_data sd1 
      JOIN products_data p1 ON sd1.ProductKey = p1.ProductKey)) * 100 AS Contribution_Percentage
FROM 
    sales_data sd
JOIN 
    products_data p ON sd.ProductKey = p.ProductKey
JOIN 
    stores_data s ON sd.StoreKey = s.StoreKey
GROUP BY 
    s.StoreKey, s.State, s.Country
ORDER BY 
    Contribution_Percentage DESC;

/** Monthly Sales Trends by Store **/
SELECT 
    sd.StoreKey,
    s.State,
    s.Country,
    DATE_FORMAT(sd.Order_Date, '%Y-%m') AS Sales_Month,
    SUM(sd.Quantity * p.Unit_Price_USD) AS Total_Sales
FROM 
    sales_data sd
JOIN 
    products_data p ON sd.ProductKey = p.ProductKey
JOIN 
    stores_data s ON sd.StoreKey = s.StoreKey
GROUP BY 
    sd.StoreKey, s.State, s.Country, Sales_Month
ORDER BY 
    Sales_Month, Total_Sales DESC;
