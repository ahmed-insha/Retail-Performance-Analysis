use intern_RETAIL_PROJ1;

/*****1. Perform Detailed exploratory analysis *****/

SELECT
    COUNT(DISTINCT order_id) AS Number_of_Orders,
    SUM(Discount) AS Total_Discount,
	SUM([Total Amount]) as Total_Revenue, 
	SUM((MRP - [Cost Per Unit] - Discount) * Quantity) as Total_Profit
FROM Orders;


SELECT COUNT(DISTINCT  product_id) as Total_products
FROM ProductsInfo

SELECT COUNT(DISTINCT  product_id) as Total_products_purchased FROM Orders

--------AVGDiscountperOrder---- AVGOrderValue------
SELECT AVG(Total_Discount_per_Order), AVG(Total_Order_Value)
FROM
(SELECT order_id, SUM(Discount) AS Total_Discount_per_Order,
    SUM([Total Amount]) AS Total_Order_Value
FROM Orders
GROUP BY order_id) as result

--------AVGSales_per_Cust--------
SELECT AVG(Sales_per_Cust)
FROM
(SELECT Customer_id,
    SUM([Total Amount]) AS Sales_per_Cust
FROM Orders
GROUP BY Customer_id) as result


-------TOTALS-------
SELECT
    COUNT(DISTINCT CustomerID) AS Number_of_Customers,
    SUM(Quantity) AS Total_Quantity,
    COUNT(DISTINCT ProductID) AS Total_Products,
    COUNT(DISTINCT DeliveredStoreID) AS Total_Stores,
    COUNT(DISTINCT StoreCity) AS Total_Locations,
    COUNT(DISTINCT Region) AS Total_Regions,
    COUNT(DISTINCT Channel) AS Total_Channels,
    COUNT(DISTINCT PaymentType) AS Total_Payment_Methods
FROM OrderLevel

SELECT COUNT(DISTINCT StoreID) AS Total_Stores
FROM StoresInfo;

SELECT COUNT(DISTINCT [Category]) AS Total_Categories
FROM ProductsInfo;

SELECT COUNT(DISTINCT payment_type) AS Total_Stores
FROM OrderPayments;


------------AverageDiscountPerCustomer---------------
SELECT AVG(AverageDiscountPerCustomer) FROM(
SELECT 
    Customer_id,
    AVG(Discount) AS AverageDiscountPerCustomer
FROM Orders
GROUP BY Customer_id) sub


-----------AverageCategoriesPerOrder-----
SELECT 
    AVG(CategoriesPerOrder) AS AverageCategoriesPerOrder
FROM (
    SELECT 
        o.order_id,
        COUNT(DISTINCT p.Category) AS CategoriesPerOrder
    FROM Orders o
    JOIN ProductsInfo p ON o.product_id = p.product_id
    GROUP BY o.order_id
) AS SubQuery;

--------AverageItemsPerOrder--------
SELECT 
    CONVERT(FLOAT,AVG(ItemsPerOrder)) AS AverageItemsPerOrder
FROM (
    SELECT 
        order_id,
        SUM(Quantity) AS ItemsPerOrder
    FROM Orders
    GROUP BY order_id
) AS SubQuery;

-------TransactionsPerCustomer----------
SELECT 
    AVG(Transactions) AS TransactionsPerCustomer
FROM (
    SELECT 
        Customer_id,
        COUNT(order_id) AS Transactions
    FROM Orders
    GROUP BY Customer_id
) AS SubQuery;

--------AvgProfitPerCustomer----------
SELECT AVG(ProfitPerCustomer) FROM
(SELECT 
    Customer_id,
    SUM([Total Amount] - [Cost Per Unit] * Quantity) AS ProfitPerCustomer
FROM Orders
GROUP BY Customer_id) sub

----- Total Cost & Quantity
SELECT 
    SUM([Cost Per Unit] * Quantity) AS TotalCost,
	SUM(Quantity) AS TotalQuantity
FROM Orders;

----------Locations & Regions--------
SELECT 
    COUNT(DISTINCT seller_city) AS TotalLocations
FROM StoresInfo;

SELECT 
    COUNT(DISTINCT seller_city) AS TotalLocations
FROM Orders o
LEFT JOIN StoresInfo si ON o.Delivered_StoreID = si.StoreID

SELECT 
    COUNT(DISTINCT Region) AS TotalRegions
FROM StoresInfo;

---------Percentages Profit & Discount
SELECT 
    SUM([Total Amount] - [Cost Per Unit] * Quantity) / SUM([Total Amount]) * 100 AS PercentageOfProfit,
	SUM(Discount) / SUM([Total Amount]) * 100 AS PercentageOfDiscount
FROM Orders;

------RepeatCustomerPercentage---------------
WITH RepeatCustomers AS (
    SELECT Customer_id
    FROM Orders
    GROUP BY Customer_id
    HAVING COUNT(DISTINCT order_id) > 1
)
SELECT 
    (COUNT(DISTINCT rc.Customer_id) * 1.0 / 
    (SELECT COUNT(DISTINCT Customer_id) FROM Orders)) * 100 AS RepeatCustomerPercentage
FROM RepeatCustomers rc;

----------RepeatPurchaseRate-------
SELECT 
    (COUNT(DISTINCT Order_id) * 1.0 / 
    (SELECT COUNT(DISTINCT Order_id) FROM Orders)) * 100 AS RepeatPurchaseRate
FROM Orders
WHERE Customer_id IN (
    SELECT Customer_id
    FROM Orders
    GROUP BY Customer_id
    HAVING COUNT(order_id) > 1
);

---------OneTimeBuyersPercentage-------
WITH OneTimeBuyers AS (
    SELECT Customer_id
    FROM Orders
    GROUP BY Customer_id
    HAVING COUNT(DISTINCT order_id) = 1
)
SELECT 
    (COUNT(DISTINCT otb.Customer_id) * 1.0 / 
    (SELECT COUNT(DISTINCT Customer_id) FROM Orders)) * 100 AS OneTimeBuyersPercentage
FROM OneTimeBuyers otb;

----------AvgDaysBetweenTransactions-------------
WITH CustomerTransactions AS (
    SELECT 
        Customer_id,
        Bill_date_timestamp,
        LAG(Bill_date_timestamp) OVER (PARTITION BY Customer_id ORDER BY Bill_date_timestamp) AS PreviousTransactionDate
    FROM Orders
),
DaysBetweenTransactions AS (
    SELECT 
        Customer_id,
        DATEDIFF(DAY, PreviousTransactionDate, Bill_date_timestamp) AS DaysBetween
    FROM CustomerTransactions
    WHERE PreviousTransactionDate IS NOT NULL
)
SELECT 
    AVG(DaysBetween) AS AvgDaysBetweenTransactions
FROM DaysBetweenTransactions;


/*************/

-- Popular Categories by Store
SELECT 
    s.StoreID, 
    p.Category, 
    COUNT(o.product_id) AS Popularity
FROM Orders o
JOIN ProductsInfo p ON o.product_id = p.product_id
JOIN StoresInfo s ON o.Delivered_StoreID = s.StoreID
GROUP BY s.StoreID, p.Category
ORDER BY Popularity DESC;


-- Popular Categories by State
SELECT 
    s.seller_state, 
    p.Category, 
    COUNT(o.product_id) AS Popularity
FROM Orders o
JOIN ProductsInfo p ON o.product_id = p.product_id
JOIN StoresInfo s ON o.Delivered_StoreID = s.StoreID
GROUP BY s.seller_state, p.Category
ORDER BY Popularity DESC;

-- Popular Categories by Region
SELECT 
    s.Region, 
    p.Category, 
    COUNT(o.product_id) AS Popularity
FROM Orders o
JOIN ProductsInfo p ON o.product_id = p.product_id
JOIN StoresInfo s ON o.Delivered_StoreID = s.StoreID
GROUP BY s.Region, p.Category
ORDER BY Popularity DESC;

----------
WITH RankedCategories AS (
    SELECT 
        s.Region, 
        p.Category, 
        COUNT(o.product_id) AS Popularity,
        ROW_NUMBER() OVER (PARTITION BY s.Region ORDER BY COUNT(o.product_id) DESC) AS Rank
    FROM Orders o
    JOIN ProductsInfo p ON o.product_id = p.product_id
    JOIN StoresInfo s ON o.Delivered_StoreID = s.StoreID
    GROUP BY s.Region, p.Category
)
SELECT 
    Region, 
    Category,
	Rank,
    Popularity
FROM RankedCategories
WHERE Rank <= 3
ORDER BY Region, Rank;

----------

-- Popular Products by Store
SELECT 
    s.StoreID, 
    p.product_id, 
    COUNT(o.product_id) AS Popularity
FROM Orders o
JOIN ProductsInfo p ON o.product_id = p.product_id
JOIN StoresInfo s ON o.Delivered_StoreID = s.StoreID
GROUP BY s.StoreID, p.product_id
ORDER BY Popularity DESC;

-- Popular Products by State
SELECT 
    s.seller_state, 
    p.product_id, 
    COUNT(o.product_id) AS Popularity
FROM Orders o
JOIN ProductsInfo p ON o.product_id = p.product_id
JOIN StoresInfo s ON o.Delivered_StoreID = s.StoreID
GROUP BY s.seller_state, p.product_id
ORDER BY Popularity DESC;

-- Popular Products by Region
SELECT 
    s.Region, 
    p.product_id, 
    COUNT(o.product_id) AS Popularity
FROM Orders o
JOIN ProductsInfo p ON o.product_id = p.product_id
JOIN StoresInfo s ON o.Delivered_StoreID = s.StoreID
GROUP BY s.Region, p.product_id
ORDER BY Popularity DESC;

-----------------

--------Top 10 expensive products
SELECT TOP 10
    o.product_id,
	[Cost Per Unit],
    SUM((MRP-Discount)*Quantity) AS total_sales,
	ROUND(SUM((MRP-Discount)*Quantity) / (SELECT SUM((MRP-Discount)*Quantity) FROM Orders) * 100,3)
	AS sales_contribution_percentage
FROM ProductsInfo pin
JOIN Orders o ON pin.product_id = o.product_id
GROUP BY o.product_id, [Cost Per Unit]
ORDER BY [Cost Per Unit] DESC

-----------Top 10 best & worst performing stores
SELECT TOP 10
    StoreID,
	SellerCity, SellerState, Region,
    SUM(TotalRevenue) AS TotalRevenue
FROM StoreLevel
GROUP BY StoreID, SellerCity, SellerState, Region
ORDER BY TotalRevenue DESC

-----------Bottom 10 worst performing stores

SELECT TOP 10
    DeliveredStoreID,
	StoreState, Region,
    SUM(TotalAmount) AS TotalRevenue
FROM OrderLevel
GROUP BY DeliveredStoreID, StoreCity, StoreState, Region
ORDER BY TotalRevenue ASC

----------Stores regions, states, cities
SELECT Region, StoreState, COUNT(DISTINCT StoreCity) as No_of_Cities,COUNT(DISTINCT DeliveredStoreID) as No_of_Stores
FROM OrderLevel
GROUP BY Region, StoreState
ORDER BY Region

SELECT Region, SellerState, COUNT(DISTINCT SellerCity) as No_of_Cities,COUNT(DISTINCT StoreID) as No_of_Stores
FROM StoreLevel
GROUP BY Region, SellerState
ORDER BY Region

------------------

/*******2. CUSTOMER BEHAVIOR ANALYsIS*******/

----------Gender-wise distribution
SELECT Gender, COUNT(Custid) as No_of_Customers 
FROM CustomerLevel
GROUP BY Gender

SELECT COUNT(DISTINCT Customer_id) from Orders

/******Preferences of Customers (discount preference etc.)*******/

-------Prefered ProductCategory
SELECT TOP 1 ProductCategory
FROM OrderLevel
GROUP BY ProductCategory
ORDER BY COUNT(ProductCategory) DESC

-------Prefered Store
SELECT DeliveredStoreID, 
FROM OrderLevel
GROUP BY DeliveredStoreID
ORDER BY COUNT(DeliveredStoreID) DESC

WITH MostPreferredStore AS (
    SELECT TOP 1 DeliveredStoreID
    FROM OrderLevel
    GROUP BY DeliveredStoreID
    ORDER BY COUNT(*) DESC
),
CustomerCountForMostPreferredStore AS (
    SELECT COUNT(DISTINCT CustomerID) AS UniqueCustomerCount
    FROM OrderLevel
    WHERE DeliveredStoreID = (SELECT DeliveredStoreID FROM MostPreferredStore)
),
TotalUniqueCustomers AS (
    SELECT COUNT(DISTINCT CustomerID) AS TotalUniqueCustomerCount
    FROM OrderLevel
)

SELECT 
    (SELECT UniqueCustomerCount FROM CustomerCountForMostPreferredStore) * 1.0 / 
    (SELECT TotalUniqueCustomerCount FROM TotalUniqueCustomers) * 100 AS PercentageOfCustomers

-------Prefered payment method-------
SELECT TOP 1 PaymentType
FROM OrderLevel
GROUP BY PaymentType
ORDER BY COUNT(*) DESC

-------Prefered channel-------
SELECT TOP 1 Channel
FROM OrderLevel
GROUP BY Channel
ORDER BY COUNT(Channel) DESC

---------customers who purchased in all the channels 
WITH CustomerChannels AS (
    SELECT
        CustomerID,
        Channel
    FROM OrderLevel
    GROUP BY CustomerID, Channel
),

ChannelsPerCustomer AS (
    SELECT
        CustomerID,
        COUNT(DISTINCT Channel) AS ChannelCount
    FROM CustomerChannels
    GROUP BY CustomerID
),

UniqueChannels AS (
    SELECT COUNT(DISTINCT Channel) AS TotalChannels
    FROM OrderLevel
),

CustomersAllChannels AS (
    SELECT
        cpc.CustomerID
    FROM ChannelsPerCustomer cpc
    CROSS JOIN UniqueChannels uc
    WHERE cpc.ChannelCount = uc.TotalChannels
)

-- Calculate Key Metrics for Customers in All Channels
SELECT
    c.CustomerID,
    COUNT(o.OrderID) AS TotalOrders,
    SUM(o.TotalAmount) AS TotalSpent,
    AVG(o.CustomerSatisfactionScore) AS AvgSatisfactionScore,
    COUNT(DISTINCT o.Channel) AS ChannelsPurchased
FROM CustomersAllChannels c
JOIN OrderLevel o ON c.CustomerID = o.CustomerID
GROUP BY c.CustomerID
ORDER BY TotalSpent DESC;

---------Understand the behavior of customers who purchased one category and purchased multiple categories

WITH CustomerCategories AS (
    SELECT
        CustomerID,
        COUNT(DISTINCT ProductCategory) AS CategoryCount
    FROM OrderLevel
    GROUP BY CustomerID
)
(SELECT
    'One Category' AS CategoryGroup,
    COUNT(DISTINCT o.CustomerID) AS NumberOfCustomers,
    COUNT(o.OrderID) AS TotalOrders,
    ROUND(SUM(o.TotalAmount),2) AS TotalSpent,
    ROUND(AVG(o.CustomerSatisfactionScore),2) AS AvgSatisfactionScore
FROM CustomerCategories cc
JOIN OrderLevel o ON cc.CustomerID = o.CustomerID
WHERE cc.CategoryCount = 1)
UNION ALL
(SELECT
    'Multiple Categories' AS CategoryGroup,
    COUNT(DISTINCT o.CustomerID) AS NumberOfCustomers,
    COUNT(o.OrderID) AS TotalOrders,
    ROUND(SUM(o.TotalAmount),2) AS TotalSpent,
    ROUND(AVG(o.CustomerSatisfactionScore),2) AS AvgSatisfactionScore
FROM CustomerCategories cc
JOIN OrderLevel o ON cc.CustomerID = o.CustomerID
WHERE cc.CategoryCount > 1)


---------Understand the behavior of one time buyers and repeat buyers
WITH CustomerPurchases AS (
    SELECT
        CustomerID,
        COUNT(OrderID) AS PurchaseCount
    FROM OrderLevel
    GROUP BY CustomerID
)
(SELECT
    'One-Time Buyers' AS BuyerGroup,
    COUNT(DISTINCT o.CustomerID) AS NumberOfCustomers,
    COUNT(o.OrderID) AS TotalOrders,
    ROUND(SUM(o.TotalAmount),2) AS TotalSpent,
    ROUND(AVG(o.CustomerSatisfactionScore),2) AS AvgSatisfactionScore
FROM CustomerPurchases cp
JOIN OrderLevel o ON cp.CustomerID = o.CustomerID
WHERE cp.PurchaseCount = 1
)
UNION ALL
(SELECT
    'Repeat Buyers' AS BuyerGroup,
    COUNT(DISTINCT o.CustomerID) AS NumberOfCustomers,
    COUNT(o.OrderID) AS TotalOrders,
    ROUND(SUM(o.TotalAmount),2) AS TotalSpent,
    ROUND(AVG(o.CustomerSatisfactionScore),2) AS AvgSatisfactionScore
FROM CustomerPurchases cp
JOIN OrderLevel o ON cp.CustomerID = o.CustomerID
WHERE cp.PurchaseCount > 1)


---------Avg CUSTOMER ACQUISITION---------

----Identify the month and year of each customer's first transaction
WITH CustomerFirstTransaction AS (
    SELECT 
        o.Customer_id,
        MIN(o.Bill_date_timestamp) AS FirstTransactionDate
    FROM Orders o
    GROUP BY o.Customer_id
),

--  Extract the year and month from the first transaction date
CustomerFirstTransactionYearMonth AS (
    SELECT
        Customer_id,
        FORMAT(FirstTransactionDate, 'yyyy-MM') AS YearMonth
    FROM CustomerFirstTransaction
),

--  Count the number of new customers acquired each month
NewCustomersPerMonth AS (
    SELECT
        YearMonth,
        COUNT(Customer_id) AS NewCustomerCount
    FROM CustomerFirstTransactionYearMonth
    GROUP BY YearMonth
)

--  Calculate the average number of new customers acquired each month
SELECT
    AVG(NewCustomerCount) AS AvgNewCustomersPerMonth
FROM NewCustomersPerMonth;

----MOnth-wise
-- Identify the month and year of each customer's first transaction
WITH CustomerFirstTransaction AS (
    SELECT 
        o.Customer_id,
        MIN(o.Bill_date_timestamp) AS FirstTransactionDate
    FROM Orders o
    GROUP BY o.Customer_id
),

-- Extract the year and month from the first transaction date
CustomerFirstTransactionYearMonth AS (
    SELECT
        Customer_id,
        FORMAT(FirstTransactionDate, 'yyyy-MM') AS YearMonth
    FROM CustomerFirstTransaction
),

-- Count the number of new customers acquired each month
NewCustomersPerMonth AS (
    SELECT
        YearMonth,
        COUNT(Customer_id) AS NewCustomerCount
    FROM CustomerFirstTransactionYearMonth
    GROUP BY YearMonth
)

--  Select the year-month and number of new customers acquired each month
SELECT
    YearMonth,
    NewCustomerCount
FROM NewCustomersPerMonth
ORDER BY YearMonth;



-------------------------


/*****Customer RFM Segmentation******/

WITH MaxDate AS (
    SELECT MAX(Bill_date_timestamp) AS MaxBillDate
    FROM Orders
),
RFM_Calculation AS (
    SELECT 
        Customer_id,
        DATEDIFF(DAY, MAX(Bill_date_timestamp), (SELECT MaxBillDate FROM MaxDate)) AS Recency,  -- Recency
        COUNT(order_id) AS Frequency,  -- Frequency
        SUM([Total Amount]) AS Monetary  -- Monetary
    FROM Orders
    GROUP BY Customer_id
),

-- Step 2: Assign RFM Scores
RFM_Scores AS (
    SELECT 
        Customer_id,
        Recency,
        Frequency,
        Monetary,
        NTILE(4) OVER (ORDER BY Recency ASC) AS RecencyScore,
        NTILE(4) OVER (ORDER BY Frequency DESC) AS FrequencyScore,
        NTILE(4) OVER (ORDER BY Monetary DESC) AS MonetaryScore
    FROM RFM_Calculation
),

-- Step 3: Assign Segments based on RFM Scores
RFM_Segmentation AS (
    SELECT 
        rs.Customer_id,
        rs.Recency,
        rs.Frequency,
        rs.Monetary,
        rs.RecencyScore,
        rs.FrequencyScore,
        rs.MonetaryScore,
        (rs.RecencyScore + rs.FrequencyScore + rs.MonetaryScore) AS RFMSum,
        CASE 
            WHEN (rs.RecencyScore + rs.FrequencyScore + rs.MonetaryScore) >= 9 THEN 'Standard'
            WHEN (rs.RecencyScore + rs.FrequencyScore + rs.MonetaryScore) >= 6 THEN 'Silver'
            WHEN (rs.RecencyScore + rs.FrequencyScore + rs.MonetaryScore) >= 4 THEN 'Gold'
            ELSE 'Premium'
        END AS Segment
    FROM RFM_Scores rs
),

-- Step 4: Calculate additional metrics for each segment
Segmentation_Metrics AS (
    SELECT 
        rs.Segment,
        SUM(rs.Monetary) AS Total_revenue,
        AVG(rs.Frequency) AS Average_purchase_frequency,
        AVG(o.[Total Amount]) AS Avg_order_value,
        AVG(orr.Customer_Satisfaction_Score) AS Avg_customer_satisfaction_score,
        COUNT(rs.Customer_id) AS No_of_customers,
        AVG(rs.Recency) AS Avg_days_between_orders
    FROM RFM_Segmentation rs
    JOIN Orders o ON rs.Customer_id = o.Customer_id
    JOIN OrderReview_Ratings orr ON o.order_id = orr.order_id
    GROUP BY rs.Segment
)

-- Final Query: Select RFM Segmentation Result with Metrics and Order by Segment
SELECT 
    Segment,
    Total_revenue,
    Average_purchase_frequency,
    Avg_order_value,
    Avg_customer_satisfaction_score,
    No_of_customers,
    Avg_days_between_orders
FROM Segmentation_Metrics
ORDER BY 
    CASE 
        WHEN Segment = 'Premium' THEN 1
        WHEN Segment = 'Gold' THEN 2
        WHEN Segment = 'Silver' THEN 3
        ELSE 4
    END;

/***RFM Calculation: calculates Recency, Frequency, and Monetary values for each customer based on their orders.

RFM Scores: Assigns scores to each customer based on quartiles of R, F, M values

RFM Segmentation: Segments customers into categories (Premium, Gold, Silver, Standard) based on their RFM scores.***/

-----------------------------------


/**********3. CROSS_SELLING (Which products are selling together)*****************************/
WITH ProductCombinations AS (
    SELECT
        o1.order_id,
        o1.product_id AS product_id1,
        o2.product_id AS product_id2,
        o3.product_id AS product_id3,
        COUNT(*) AS combination_count
    FROM Orders o1
    JOIN Orders o2 ON o1.order_id = o2.order_id AND o1.product_id < o2.product_id
    LEFT JOIN Orders o3 ON o2.order_id = o3.order_id AND o2.product_id < o3.product_id
    GROUP BY o1.order_id, o1.product_id, o2.product_id, o3.product_id
),

TopProductCombinations AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY combination_count DESC) AS rank,
        product_id1,
        product_id2,
        product_id3,
        combination_count
    FROM ProductCombinations
    WHERE product_id2 IS NOT NULL
)

SELECT
    rank,
    CASE
        WHEN product_id3 IS NULL THEN CONCAT(product_id1, '-', product_id2)
        ELSE CONCAT(product_id1, '-', product_id2, '-', product_id3)
    END AS product_combination,
    combination_count
FROM TopProductCombinations
WHERE rank <= 10;

-----------Categories selling together
WITH ProductCategoryCombinations AS (
    SELECT
        o1.OrderID,
        o1.ProductCategory AS ProductCategory1,
        o2.ProductCategory AS ProductCategory2,
        o3.ProductCategory AS ProductCategory3,
        COUNT(*) AS combination_count
    FROM OrderLevel o1
    JOIN OrderLevel o2 ON o1.OrderID = o2.OrderID AND o1.ProductCategory < o2.ProductCategory
    LEFT JOIN OrderLevel o3 ON o2.OrderID = o3.OrderID AND o2.ProductCategory < o3.ProductCategory
    GROUP BY o1.OrderID, o1.ProductCategory, o2.ProductCategory, o3.ProductCategory
),

TopProductCategoryCombinations AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY combination_count DESC) AS rank,
        ProductCategory1,
        ProductCategory2,
        ProductCategory3,
        combination_count
    FROM ProductCategoryCombinations
    WHERE ProductCategory2 IS NOT NULL
)

SELECT
    rank,
    CASE
        WHEN ProductCategory3 IS NULL THEN CONCAT(ProductCategory1, '-', ProductCategory2)
        ELSE CONCAT(ProductCategory1, '-', ProductCategory2, '-', ProductCategory3)
    END AS product_category_combination,
    combination_count
FROM TopProductCategoryCombinations
WHERE rank <= 10;


/************4. CATEGORY BEHAVIOR******************/

--------Total Sales & Percentage of sales by category (Perform Pareto Analysis)
WITH CategorySales AS (
    SELECT 
        ProductCategory AS Category,
        SUM(TotalAmount) AS Sales
    FROM OrderLevel
    GROUP BY ProductCategory
),

-- Calculate the total sales for the percentage calculation
TotalSales AS (
    SELECT 
        SUM(Sales) AS TotalSales
    FROM CategorySales
),

-- Calculate the cumulative sales and cumulative sales percentage
CumulativeSales AS (
    SELECT 
        cs.Category,
        cs.Sales,
        SUM(cs.Sales) OVER (ORDER BY cs.Sales DESC) AS CumulativeSales,
        SUM(cs.Sales) OVER (ORDER BY cs.Sales DESC) * 100.0 / ts.TotalSales AS CumulativeSalesPercentage
    FROM 
        CategorySales cs, TotalSales ts
)

-- Final select statement to output the results
SELECT 
    Category,
    Sales,
    CumulativeSales,
    CumulativeSalesPercentage
--INTO CategorySales_pareto
FROM 
    CumulativeSales
ORDER BY 
    Sales DESC;

----------

--------Most profitable category and contribution
WITH CategoryRevenue AS (
    SELECT
        ol.ProductCategory,
        SUM(ol.TotalAmount) AS TotalRevenue,
        SUM(ol.CostPerUnit * ol.Quantity) AS TotalCost
    FROM OrderLevel ol
    GROUP BY ol.ProductCategory
),

CategoryContribution AS (
    SELECT
        cr.ProductCategory,
        cr.TotalRevenue,
        cr.TotalCost,
        cr.TotalRevenue - cr.TotalCost AS Profit,
        (cr.TotalRevenue / (SELECT SUM(TotalRevenue) FROM CategoryRevenue)) * 100 AS ContributionPercentage
    FROM CategoryRevenue cr
)

SELECT TOP 1
    cc.ProductCategory,
    cc.TotalRevenue,
    cc.Profit,
    cc.ContributionPercentage
FROM CategoryContribution cc
ORDER BY cc.TotalRevenue DESC;

----------Category Penetration Analysis------ (CHART)
WITH MonthlyCategoryOrders AS (
    SELECT
        FORMAT(BillDateTimestamp, 'yyyy-MM') AS YearMonth,
        ProductCategory,
        COUNT(DISTINCT OrderID) AS OrdersWithCategory
    FROM OrderLevel
    GROUP BY FORMAT(BillDateTimestamp, 'yyyy-MM'), ProductCategory
),

MonthlyTotalOrders AS (
    SELECT
        FORMAT(BillDateTimestamp, 'yyyy-MM') AS YearMonth,
        COUNT(DISTINCT OrderID) AS TotalOrders
    FROM OrderLevel
    GROUP BY FORMAT(BillDateTimestamp, 'yyyy-MM')
),

CategoryPenetration AS (
    SELECT
        mco.YearMonth,
        mco.ProductCategory,
        mco.OrdersWithCategory,
        mto.TotalOrders,
        ROUND((CAST(mco.OrdersWithCategory AS FLOAT) / mto.TotalOrders) * 100, 2) AS PenetrationPercentage
    FROM MonthlyCategoryOrders mco
    JOIN MonthlyTotalOrders mto ON mco.YearMonth = mto.YearMonth
)

SELECT
    YearMonth,
    ProductCategory,
    OrdersWithCategory,
    TotalOrders,
    PenetrationPercentage
FROM CategoryPenetration
ORDER BY YearMonth, PenetrationPercentage DESC;


----------Most popular category during first purchase of customer
WITH FirstPurchase AS (
    SELECT
        CustomerID,
        MIN(BillDateTimestamp) AS FirstPurchaseDate
    FROM OrderLevel
    GROUP BY CustomerID
),

FirstPurchaseDetails AS (
    SELECT
        fp.CustomerID,
        ol.ProductCategory,
        ROW_NUMBER() OVER (PARTITION BY fp.CustomerID ORDER BY ol.BillDateTimestamp) AS PurchaseRank
    FROM FirstPurchase fp
    JOIN OrderLevel ol ON fp.CustomerID = ol.CustomerID AND fp.FirstPurchaseDate = ol.BillDateTimestamp
)

SELECT TOP 1
    fpd.ProductCategory AS MostPopularCategoryDuringFirstPurchase,
    COUNT(*) AS Frequency
FROM FirstPurchaseDetails fpd
WHERE fpd.PurchaseRank = 1  -- Only consider the first purchase
GROUP BY fpd.ProductCategory
ORDER BY Frequency DESC;

----------Cross Category Analysis-----(CHART)
WITH OrderCategoryCount AS (
    SELECT
        OrderID,
        COUNT(DISTINCT ProductCategory) AS NumCategories
    FROM OrderLevel
    GROUP BY OrderID
),

MonthlyAverageCategories AS (
    SELECT
        FORMAT(ol.BillDateTimestamp, 'yyyy-MM') AS YearMonth,
        ol.Region,
        ol.StoreState,
        AVG(occ.NumCategories) AS AvgCategoriesPerOrder
    FROM OrderLevel ol
    JOIN OrderCategoryCount occ ON ol.OrderID = occ.OrderID
    GROUP BY FORMAT(ol.BillDateTimestamp, 'yyyy-MM'), ol.Region, ol.StoreState
)

SELECT
    YearMonth,
    Region,
    StoreState,
    AvgCategoriesPerOrder
FROM MonthlyAverageCategories
ORDER BY YearMonth, Region, StoreState;



/************5. CUSTOMER SATISFACTION towards category & product ****************/

-----Which categories (top 10) are maximum rated & minimum rated and average rating score? 
-- Calculate average satisfaction score by category

-- Select top 10 maximum rated categories
WITH CategoryRatings AS (
    SELECT
        ProductCategory,
        AVG(CustomerSatisfactionScore) AS AvgSatisfactionScore
    FROM OrderLevel
    GROUP BY ProductCategory
),

-- Calculate average satisfaction score by product
ProductRatings AS (
    SELECT
        ProductID,
        ProductCategory,
        AVG(CustomerSatisfactionScore) AS AvgSatisfactionScore
    FROM OrderLevel
    GROUP BY ProductID, ProductCategory
)

SELECT
    'Category' AS Type,
    ProductCategory AS Name,
    AvgSatisfactionScore
FROM CategoryRatings
ORDER BY AvgSatisfactionScore DESC
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY

-- Select top 10 minimum rated categories
WITH CategoryRatings AS (
    SELECT
        ProductCategory,
        AVG(CustomerSatisfactionScore) AS AvgSatisfactionScore
    FROM OrderLevel
    GROUP BY ProductCategory
),
-- Calculate average satisfaction score by product
ProductRatings AS (
    SELECT
        ProductID,
        ProductCategory,
        AVG(CustomerSatisfactionScore) AS AvgSatisfactionScore
    FROM OrderLevel
    GROUP BY ProductID, ProductCategory
)
-- Select top 10 minimum rated categories
SELECT
    'Category' AS Type,
    ProductCategory AS Name,
    AvgSatisfactionScore
FROM CategoryRatings
ORDER BY AvgSatisfactionScore ASC
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY


-- Select top 10 maximum rated products
WITH CategoryRatings AS (
    SELECT
        ProductCategory,
        AVG(CustomerSatisfactionScore) AS AvgSatisfactionScore
    FROM OrderLevel
    GROUP BY ProductCategory
),
-- Calculate average satisfaction score by product
ProductRatings AS (
    SELECT
        ProductID,
        ProductCategory,
        AVG(CustomerSatisfactionScore) AS AvgSatisfactionScore
    FROM OrderLevel
    GROUP BY ProductID, ProductCategory
)
SELECT
    'Product' AS Type,
    ProductID AS Name,
	ProductCategory AS CategoryName,
    AvgSatisfactionScore
FROM ProductRatings
ORDER BY AvgSatisfactionScore DESC
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY

-- Select top 10 minimum rated products
WITH CategoryRatings AS (
    SELECT
        ProductCategory,
        AVG(CustomerSatisfactionScore) AS AvgSatisfactionScore
    FROM OrderLevel
    GROUP BY ProductCategory
),
-- Calculate average satisfaction score by product
ProductRatings AS (
    SELECT
        ProductID,
        ProductCategory,
        AVG(CustomerSatisfactionScore) AS AvgSatisfactionScore
    FROM OrderLevel
    GROUP BY ProductID, ProductCategory
)
SELECT
    'Product' AS Type,
    ProductID AS Name,
	ProductCategory AS CategoryName,
    AvgSatisfactionScore
FROM ProductRatings
ORDER BY AvgSatisfactionScore ASC
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;


--------------------
-- Average satisfaction score by Region and StoreState
SELECT
    Region,
    --StoreState,
    AVG(CustomerSatisfactionScore) AS AvgSatisfactionScore
FROM OrderLevel
GROUP BY Region--, StoreState
ORDER BY AvgSatisfactionScore DESC;

-- Average satisfaction score by DeliveredStoreID
SELECT
    DeliveredStoreID,
    AVG(CustomerSatisfactionScore) AS AvgSatisfactionScore
FROM OrderLevel
GROUP BY DeliveredStoreID
ORDER BY AvgSatisfactionScore DESC;

-- Average satisfaction score by ProductID
SELECT
    ProductID,
    AVG(CustomerSatisfactionScore) AS AvgSatisfactionScore
FROM OrderLevel
GROUP BY ProductID
ORDER BY AvgSatisfactionScore DESC;


-- Average satisfaction score by ProductCategory
SELECT
    ProductCategory,
    AVG(CustomerSatisfactionScore) AS AvgSatisfactionScore
FROM OrderLevel
GROUP BY ProductCategory
ORDER BY AvgSatisfactionScore DESC;

-- Average satisfaction score by month
SELECT
    FORMAT(BillDateTimestamp, 'yyyy-MM') AS YearMonth,
    AVG(CustomerSatisfactionScore) AS AvgSatisfactionScore
FROM OrderLevel
GROUP BY FORMAT(BillDateTimestamp, 'yyyy-MM')
ORDER BY YearMonth ASC;




/****************6. COHORT ANALYSIS******************/


-- Cohort analysis for customer retention (Monthly Retention for Each Cohort)
--Identify the First Purchase Month for Each Customer:
WITH FirstPurchase AS (
    SELECT
        CustomerID,
        MIN(BillDateTimestamp) AS FirstPurchaseDate
    FROM OrderLevel
    GROUP BY CustomerID
),
Cohort AS (
    SELECT
        CustomerID,
        FORMAT(FirstPurchaseDate, 'yyyy-MM') AS CohortMonth
    FROM FirstPurchase
),
MonthlyRetention AS (
    SELECT
        c.CohortMonth,
        FORMAT(ol.BillDateTimestamp, 'yyyy-MM') AS RetentionMonth,
        COUNT(DISTINCT ol.CustomerID) AS RetainedCustomers
    FROM Cohort c
    JOIN OrderLevel ol ON c.CustomerID = ol.CustomerID
    GROUP BY c.CohortMonth, FORMAT(ol.BillDateTimestamp, 'yyyy-MM')
),
CohortSizes AS (
    SELECT
        CohortMonth,
        COUNT(DISTINCT CustomerID) AS CohortSize
    FROM Cohort
    GROUP BY CohortMonth
)
SELECT
    mr.CohortMonth,
    mr.RetentionMonth,
    mr.RetainedCustomers,
    cs.CohortSize,
    (CAST(mr.RetainedCustomers AS FLOAT) / cs.CohortSize) * 100 AS RetentionRate
FROM MonthlyRetention mr
JOIN CohortSizes cs ON mr.CohortMonth = cs.CohortMonth
ORDER BY mr.CohortMonth, mr.RetentionMonth;





/**************7.Sales Trends, patterns, and seasonality****************/

-------Highest and Lowest Sales Months with Sales Amount and Contribution Percentage:
WITH monthly_sales AS (
    SELECT 
        DATENAME(MONTH,Bill_date_timestamp) AS month, 
        SUM([Total Amount]) AS total_sales
    FROM Orders
    GROUP BY DATENAME(MONTH,Bill_date_timestamp)
),
total_sales AS (
    SELECT SUM(total_sales) AS total_year_sales FROM monthly_sales
)
SELECT 
    month, 
    total_sales, 
    (total_sales / total_year_sales) * 100 AS sales_contribution_percentage
FROM monthly_sales, total_sales
--ORDER BY total_sales DESC
ORDER BY  DATEPART(MONTH,Bill_date_timestamp)

-------Sales Trend by Month (yyyy-MMM)
SELECT 
    FORMAT(BillDateTimestamp, 'yyyy-MM') AS month, 
    SUM(TotalAmount) AS total_sales
FROM OrderLevel
GROUP BY FORMAT(BillDateTimestamp, 'yyyy-MM')
ORDER BY FORMAT(BillDateTimestamp, 'yyyy-MM');

--------Seasonality in Sales Days of Week
SELECT 
    DATENAME(WEEKDAY,Bill_date_timestamp) AS day_of_week, 
	((DATEPART(WEEKDAY,Bill_date_timestamp)+ 5) % 7) + 1  AS day_of_week_no,
    SUM([Total Amount]) AS total_sales
FROM Orders
GROUP BY DATENAME(WEEKDAY,Bill_date_timestamp), DATEPART(WEEKDAY,Bill_date_timestamp)
--ORDER BY total_sales DESC
ORDER BY ((DATEPART(WEEKDAY,Bill_date_timestamp)+ 5) % 7) + 1 

SELECT 
    DATENAME(WEEKDAY,Bill_date_timestamp) AS day_of_week, 
	DATEPART(WEEKDAY,Bill_date_timestamp)  AS day_of_week_no,
    SUM([Total Amount]) AS total_sales
FROM Orders
GROUP BY DATENAME(WEEKDAY,Bill_date_timestamp), DATEPART(WEEKDAY,Bill_date_timestamp)
ORDER BY total_sales DESC
ORDER BY DATEPART(WEEKDAY,Bill_date_timestamp)

-----------Total Sales Weekdays vs. Weekends:
SELECT  
    CASE 
        WHEN ((DATEPART(dw, Bill_date_timestamp) + 5) % 7) + 1 IN (6, 7) THEN 'Weekend' 
        ELSE 'Weekday' 
    END AS day_type, 
    SUM([Total Amount]) AS total_sales
FROM Orders
GROUP BY 
    CASE 
        WHEN ((DATEPART(dw, Bill_date_timestamp) + 5) % 7) + 1 IN (6, 7) THEN 'Weekend' 
        ELSE 'Weekday' 
    END
ORDER BY total_sales DESC;

---------2nd try AVG
SELECT  
    CASE 
        WHEN ((DATEPART(dw, Bill_date_timestamp) + 5) % 7) + 1 IN (6, 7) THEN 'Weekend' 
        ELSE 'Weekday' 
    END AS day_type, 
    AVG([Total Amount]) AS avg_sales
FROM Orders
GROUP BY 
    CASE 
        WHEN ((DATEPART(dw, Bill_date_timestamp) + 5) % 7) + 1 IN (6, 7) THEN 'Weekend' 
        ELSE 'Weekday' 
    END
ORDER BY avg_sales DESC;

---------Total Sales Quarter-wise
SELECT  
    DATEPART(qq, Bill_date_timestamp) AS quarter, 
    SUM([Total Amount]) AS total_sales
FROM Orders
GROUP BY DATEPART(qq, Bill_date_timestamp)
--ORDER BY total_sales DESC;
ORDER BY DATEPART(qq, Bill_date_timestamp)



