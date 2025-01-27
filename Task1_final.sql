create database intern_RETAIL_PROJ1;
use intern_RETAIL_PROJ1;

SELECT * FROM Customers
SELECT * FROM OrderPayments
SELECT * FROM OrderReview_Ratings
SELECT * FROM Orders
SELECT * FROM ProductsInfo
SELECT * FROM StoresInfo

/******** DATA CLEANING *********/

-- TABLE Customers
ALTER TABLE Customers
ALTER COLUMN Custid BIGINT NOT NULL;

ALTER TABLE Customers
ALTER COLUMN Gender CHAR(1) NULL;


-- TABLE Productsinfo
ALTER TABLE ProductsInfo
ALTER COLUMN product_name_lenght INT NULL;

ALTER TABLE ProductsInfo
ALTER COLUMN product_description_lenght INT NULL;

ALTER TABLE ProductsInfo
ALTER COLUMN product_photos_qty INT NULL;

UPDATE ProductsInfo
SET Category = 'Others' WHERE Category = '#N/A'


-- TABLE Orders
ALTER TABLE Orders
ALTER COLUMN Customer_id BIGINT NOT NULL;

ALTER TABLE Orders
ALTER COLUMN Quantity INT NULL;


/*******************DATETIME********************/
UPDATE Orders
SET Bill_date_timestamp = 
	CASE
		WHEN Bill_date_timestamp LIKE '%/%' THEN CONVERT(DATETIME, Bill_date_timestamp, 120)
		WHEN Bill_date_timestamp LIKE '%-%' THEN PARSE(Bill_date_timestamp AS DATETIME USING 'en-GB')
	END

ALTER TABLE Orders
ALTER COLUMN Bill_date_timestamp DATETIME NULL;

SELECT * FROM Orders
/************************************************/



/*************DUPLICATES**************/

-----------------Duplicate Store
SELECT COUNT(*) FROM StoresInfo
SELECT COUNT(DISTINCT StoreID) FROM StoresInfo

--ST410
SELECT StoreID, COUNT(*) FROM StoresInfo
GROUP BY StoreID
HAVING COUNT(*)>1

WITH StoreDuplicate AS(
	SELECT *, ROW_NUMBER() OVER (Partition by StoreID Order BY StoreID) as row_no
	FROM StoresInfo
	WHERE StoreID = 'ST410'
)
DELETE FROM StoreDuplicate
WHERE row_no>1;
---------------------

--Remove order_ids in OrderPayments that are not in Orders
SELECT order_id
FROM OrderPayments
EXCEPT
SELECT order_id
FROM Orders;

DELETE FROM OrderPayments
WHERE order_id NOT IN (SELECT order_id FROM Orders);



-------Remove order_ids in OrderReview_Ratings that are not in Orders
SELECT order_id
FROM OrderReview_Ratings
EXCEPT
SELECT order_id
FROM Orders;

DELETE FROM OrderReview_Ratings
WHERE order_id NOT IN (SELECT order_id FROM Orders);

----------------------------------------------

/*******Mismatch in Orders Total Amount & Payment_value********/
WITH ProblematicRows AS (
    SELECT Customer_id, order_id, product_id
    FROM Orders
    GROUP BY Customer_id, order_id, product_id
    HAVING COUNT(*) > 1
)
UPDATE Orders
SET Quantity = 1
FROM Orders o
JOIN ProblematicRows pr
	ON o.Customer_id = pr.Customer_id
	AND o.order_id = pr.order_id
	AND o.product_id = pr.product_id

UPDATE Orders
SET [Total Amount] = (MRP - Discount) * Quantity;

/***************/



/*********************************CREATING 3 NEW TABLES**********************************/

SELECT * FROM CustomerLevel
SELECT * FROM OrderLevel
SELECT * FROM StoreLevel

/*******CUSTOMER LEVEL*******/

-- Create the CustomerLevel table
CREATE TABLE CustomerLevel (
    Custid NVARCHAR(255) PRIMARY KEY,
    Gender CHAR(1),
    Location NVARCHAR(255),
    Total_Revenue FLOAT,
    Recency INT,
    Frequency INT,
    Average_Order_Value FLOAT,
    Profit FLOAT,
    Orders_Discounted INT,
    Quantity INT,
    Distinct_Items_Purchased INT
);

-- Insert initial data into CustomerLevel
INSERT INTO CustomerLevel (Custid, Gender, Location, Total_Revenue, Frequency, Quantity, Distinct_Items_Purchased)
SELECT 
    c.Custid,
    c.Gender,
    CONCAT(c.customer_city, ', ', c.customer_state) AS Location,
    SUM(o.[Total Amount]) AS Total_Revenue,
    COUNT(o.order_id) AS Frequency,
    SUM(o.Quantity) AS Quantity,
    COUNT(DISTINCT o.product_id) AS Distinct_Items_Purchased
FROM 
    Customers c
    JOIN Orders o ON c.Custid = o.Customer_id
GROUP BY 
    c.Custid, c.Gender, c.customer_city, c.customer_state;

-- Update Average Order Value
UPDATE CustomerLevel
SET Average_Order_Value = Total_Revenue / Frequency;

-- Update Profit considering the discount
UPDATE CustomerLevel
SET Profit = (
    SELECT SUM(	 (o.MRP - o.[Cost Per Unit] - o.Discount) * o.Quantity  )
    FROM Orders o
    WHERE o.Customer_id = CustomerLevel.Custid
);

-- Update Orders (discounted)
UPDATE CustomerLevel
SET Orders_Discounted = (SELECT COUNT(o.order_id)
                         FROM Orders o
                         WHERE o.Customer_id = CustomerLevel.Custid AND o.Discount > 0);


-- Update Recency
WITH MaxDate AS (
    SELECT MAX(Bill_date_timestamp) AS MaxBillDate
    FROM Orders
),
Recency_Calculation AS (
    SELECT 
        Customer_id,
        DATEDIFF(DAY, MAX(Bill_date_timestamp), (SELECT MaxBillDate FROM MaxDate)) AS Recency
    FROM Orders
    GROUP BY Customer_id
)
UPDATE CustomerLevel
SET Recency = (SELECT Recency FROM Recency_Calculation WHERE Recency_Calculation.Customer_id = CustomerLevel.Custid);


-- Verify the updates
SELECT * FROM CustomerLevel;



/*******ORDER LEVEL*******/

CREATE TABLE OrderLevel (
    OrderID NVARCHAR(255),
    CustomerID NVARCHAR(255),
    ProductID NVARCHAR(255),
    Channel NVARCHAR(255),
    DeliveredStoreID NVARCHAR(255),
    BillDateTimestamp DATETIME,
    Quantity INT,
    CostPerUnit FLOAT,
    MRP FLOAT,
    Discount FLOAT,
    TotalAmount FLOAT,
    PaymentType NVARCHAR(255),
    CustomerSatisfactionScore FLOAT,
    StoreCity NVARCHAR(255),
    StoreState NVARCHAR(255),
    Region NVARCHAR(255),
    ProductCategory NVARCHAR(255),
    ProductWeight_g FLOAT,
    ProductDimensions NVARCHAR(255)  -- Length x Height x Width
);

INSERT INTO OrderLevel (OrderID, CustomerID, ProductID, Channel, DeliveredStoreID, BillDateTimestamp, Quantity, CostPerUnit, MRP, Discount, TotalAmount, PaymentType, CustomerSatisfactionScore, StoreCity, StoreState, Region, ProductCategory, ProductWeight_g, ProductDimensions)
SELECT 
    o.order_id,
    o.Customer_id,
    o.product_id,
    o.Channel,
    o.Delivered_StoreID,
    /*TRY_CONVERT(DATETIME,*/ o.Bill_date_timestamp /*, 101)*/ AS BillDateTimestamp,
    o.Quantity,
    o.[Cost Per Unit],
    o.MRP,
    o.Discount,
    o.[Total Amount],
    op.payment_type,
    orr.Customer_Satisfaction_Score,
    si.seller_city,
    si.seller_state,
    si.Region,
    pi.Category,
    pi.product_weight_g,
    CONCAT(pi.product_length_cm, ' x ', pi.product_height_cm, ' x ', pi.product_width_cm) AS ProductDimensions
FROM 
    Orders o
LEFT JOIN OrderPayments op ON o.order_id = op.order_id
LEFT JOIN OrderReview_Ratings orr ON o.order_id = orr.order_id
LEFT JOIN StoresInfo si ON o.Delivered_StoreID = si.StoreID
LEFT JOIN ProductsInfo pi ON o.product_id = pi.product_id;

SELECT * FROM OrderLevel


/*******STORE LEVEL*******/

CREATE TABLE StoreLevel (
    StoreID NVARCHAR(255) PRIMARY KEY,
    SellerCity NVARCHAR(255),
    SellerState NVARCHAR(255),
    Region NVARCHAR(255),
    TotalOrders INT,
    TotalRevenue FLOAT,
    TotalQuantity INT,
    AverageOrderValue FLOAT,
    CustomerSatisfactionScore FLOAT,
    DistinctCustomers INT,
    DistinctProducts INT
);

INSERT INTO StoreLevel (StoreID, SellerCity, SellerState, Region, TotalOrders, TotalRevenue, TotalQuantity, AverageOrderValue, CustomerSatisfactionScore, DistinctCustomers, DistinctProducts)
SELECT 
    si.StoreID,
    si.seller_city,
    si.seller_state,
    si.Region,
    COUNT(DISTINCT o.order_id) AS TotalOrders,
    SUM(o.[Total Amount]) AS TotalRevenue,
    SUM(o.Quantity) AS TotalQuantity,
    AVG(o.[Total Amount]) AS AverageOrderValue,
    AVG(orr.Customer_Satisfaction_Score) AS CustomerSatisfactionScore,
    COUNT(DISTINCT o.Customer_id) AS DistinctCustomers,
    COUNT(DISTINCT o.product_id) AS DistinctProducts
FROM 
    StoresInfo si
LEFT JOIN Orders o ON si.StoreID = o.Delivered_StoreID
LEFT JOIN OrderReview_Ratings orr ON o.order_id = orr.order_id
GROUP BY 
    si.StoreID,
    si.seller_city,
    si.seller_state,
    si.Region;



SELECT * FROM StoreLevel