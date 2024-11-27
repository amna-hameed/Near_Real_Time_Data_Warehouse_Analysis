use metrodb;
-- Query 1 Top Revenue-Generating Products
select 
    d.Year,
    d.MonthName,
    d.IsWeekend,
    p.ProductName,
    SUM(s.Sale) AS TotalRevenue,
    SUM(s.Quantity) AS TotalQuantity
from Sales_data s
join products_data p ON s.ProductID = p.ProductID
join Dates_data d ON s.DateID = d.DateID
where d.Year = 2019
group by d.Year, d.MonthName, d.IsWeekend, p.ProductName
having TotalRevenue > 0
order by d.Year, d.MonthName, d.IsWeekend, TotalRevenue DESC
LIMIT 5;



-- Query 3 Detailed Supplier Sales Contribution
select 
    st.StoreName,
    sp.SupplierName,
    case 
        when p.ProductName like '%Clothing%' then 'Clothing'
        when p.ProductName like '%Electronics%' then 'Electronics'
        when p.ProductName like '%Food%' then 'Food'
        ELSE  'Other'
    END AS ProductCategory,
    SUM(s.Sale) AS TotalSales,
    ROUND(SUM(s.Sale) / SUM(SUM(s.Sale)) OVER (PARTITION by st.StoreName) * 100, 2) AS SalesContributionPercentage
from Sales_data s
join Stores_data st ON s.StoreID = st.StoreID
join Suppliers_data sp ON s.SupplierID = sp.SupplierID
join products_data p ON s.ProductID = p.ProductID
group by st.StoreName, sp.SupplierName, ProductCategory
order by st.StoreName, SalesContributionPercentage DESC;

-- Query 4 Seasonal Analysis of Product Sales 
select 
    p.ProductName,
    d.Season,
    st.StoreName AS Region,
    SUM(s.Sale) AS TotalSales,
    SUM(s.Quantity) AS TotalQuantitySold
from Sales_data s
join products_data p ON s.ProductID = p.ProductID
join Dates_data d ON s.DateID = d.DateID
join Stores_data st ON s.StoreID = st.StoreID
group by p.ProductName, d.Season, st.StoreName
order by TotalSales DESC;

-- Query 5 Store-Wise and Supplier-Wise Monthly Revenue Volatility
with MonthlySales AS (
    select 
        st.StoreName,
        sp.SupplierName,
        d.Year,
        d.MonthName,
        SUM(s.Sale) AS MonthlyRevenue
    from Sales_data s
    join Stores_data st ON s.StoreID = st.StoreID
    join Suppliers_data sp ON s.SupplierID = sp.SupplierID
    join Dates_data d ON s.DateID = d.DateID
    group by st.StoreName, sp.SupplierName, d.Year, d.MonthName
),
RevenueVolatility AS (
    select 
        StoreName,
        SupplierName,
        Year,
        MonthName,
        MonthlyRevenue,
        LAG(MonthlyRevenue) OVER (PARTITION by StoreName, SupplierName order by Year, MonthName) AS PreviousMonthRevenue
    from MonthlySales
)
select 
    StoreName,
    SupplierName,
    Year,
    MonthName,
    MonthlyRevenue,
    ROUND(((MonthlyRevenue - PreviousMonthRevenue) / PreviousMonthRevenue) * 100, 2) AS RevenueVolatilityPercentage
from RevenueVolatility
where PreviousMonthRevenue IS NOT NULL
order by ABS(RevenueVolatilityPercentage) DESC;


-- Query 7 Yearly Revenue Trends 
select 
    COALESCE(st.StoreName, 'All Stores') AS StoreName,
    COALESCE(sp.SupplierName, 'All Suppliers') AS SupplierName,
    COALESCE(p.ProductName, 'All Products') AS ProductName,
    SUM(s.Sale) AS TotalRevenue
from Sales_data s
join Stores_data st ON s.StoreID = st.StoreID
join Suppliers_data sp ON s.SupplierID = sp.SupplierID
join products_data p ON s.ProductID = p.ProductID
join Dates_data d ON s.DateID = d.DateID
group by
    st.StoreName, 
    sp.SupplierName, 
    p.ProductName 
with ROLLUP
order by
    StoreName, 
    SupplierName, 
    ProductName;

-- Query 8 Revenue and Volume-Based Sales Analysis 
select 
    p.ProductName,
    case 
        when d.Month between 1 AND 6 then 'H1'
        ELSE  'H2'
    END AS HalfYear,
    d.Year,
    SUM(s.Sale) AS TotalRevenue,
    SUM(s.Quantity) AS TotalQuantity
from Sales_data s
join products_data p ON s.ProductID = p.ProductID
join Dates_data d ON s.DateID = d.DateID
group by p.ProductName, HalfYear, d.Year
order by p.ProductName, d.Year, HalfYear;


-- Query 10 Create a View for Region Store Quarterly Sales 
CREATE OR REPLACE VIEW REGION_STORE_QUARTERLY_SALES AS
select 
    st.StoreName AS Region,
    d.Year,
    d.Quarter,
    SUM(s.Sale) AS QuarterlySales
from Sales_data s
join Stores_data st ON s.StoreID = st.StoreID
join Dates_data d ON s.DateID = d.DateID
group by st.StoreName, d.Year, d.Quarter
order by Region, d.Year, d.Quarter;
select * from REGION_STORE_QUARTERLY_SALES;