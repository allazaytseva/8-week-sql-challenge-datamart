# Case Study #5: Data Mart

<img src="https://user-images.githubusercontent.com/81607668/131437982-fc087a4c-0b77-4714-907b-54e0420e7166.png" alt="Image" width="500" height="520">


## Business Task
Data Mart is an online supermarket that specialises in fresh produce.

In June 2020 - large scale supply changes were made at Data Mart. All Data Mart products now use sustainable packaging methods in every single step from the farm all the way to the customer.

Danny needs your help to analyse and quantify the impact of this change on the sales performance for Data Mart and it’s separate business areas.

The key business question to answer are the following:
- What was the quantifiable impact of the changes introduced in June 2020?
- Which platform, region, segment and customer types were the most impacted by this change?
- What can we do about future introduction of similar sustainability updates to the business to minimise impact on sales?

### A. Data Cleansing Steps

#### Assignement 

In a single query, perform the following operations and generate a new table in the `data_mart` schema named `clean_weekly_sales`:
- Convert the `week_date` to a `DATE` format
- Add a `week_number` as the second column for each `week_date` value, for example any value from the 1st of January to 7th of January will be 1, 8th to 14th will be 2 etc
- Add a `month_number` with the calendar month for each `week_date` value as the 3rd column
- Add a `calendar_year` column as the 4th column containing either 2018, 2019 or 2020 values
- Add a new column called `age_band` after the original segment column using the following mapping on the number inside the segment value
  
<img width="166" alt="image" src="https://user-images.githubusercontent.com/81607668/131438667-3b7f3da5-cabc-436d-a352-2022841fc6a2.png">
  
- Add a new `demographic` column using the following mapping for the first letter in the `segment` values:  

| segment | demographic | 
| ------- | ----------- |
| C | Couples |
| F | Families |

- Ensure all `null` string values with an "unknown" string value in the original `segment` column as well as the new `age_band` and `demographic` columns
- Generate a new `avg_transaction` column as the sales value divided by transactions rounded to 2 decimal places for each record

```sql

DROP TABLE IF EXISTS clean_weekly_sales;

CREATE TEMP TABLE clean_weekly_sales AS (
  SELECT
    TO_DATE (week_date, 'DD/MM/YY') AS week_date,
    DATE_PART ('week', TO_DATE (week_date, 'DD/MM/YY')) AS week_number,
    DATE_PART ('month', TO_DATE (week_date, 'DD/MM/YY')) AS month_number,
    DATE_PART ('year', TO_DATE (week_date, 'DD/MM/YY')) AS calendar_year,
    region,
    platform,
    segment,
    CASE
      WHEN RIGHT (segment, 1) = '1' THEN 'Young Adults'
      WHEN RIGHT (segment, 1) = '2' THEN 'Middle Aged'
      WHEN RIGHT (segment, 1) IN ('3', '4') THEN 'Retirees'
      ELSE 'Unknown'
    END AS age_band,
    CASE
      WHEN LEFT (segment, 1) = 'C' THEN 'Couples'
      WHEN LEFT (segment, 1) = 'F' THEN 'Families'
      ELSE 'Unknown'
    END AS demographic,
    transactions,
    sales,
    customer_type,
    ROUND((sales :: NUMERIC / transactions), 2) AS avg_transactions
  FROM
    data_mart.weekly_sales
);


SELECT
  *
FROM
  clean_weekly_sales
```

Here's the result: 

![Screen Shot 2022-12-08 at 12 34 34 PM](https://user-images.githubusercontent.com/95102899/206564661-ebf2677b-f1a6-4a1e-96a3-115819322ceb.png)

_______________________________________________________________________________________________________________________________________________________
_______________________________________________________________________________________________________________________________________________________


### B. Data Exploration 


**1. What day of the week is used for each week_date value?**

```sql

SELECT DATE_PART ('dow', week_date), TO_CHAR (week_date, 'Day') AS dow
FROM clean_weekly_sales
```

![Screen Shot 2022-12-08 at 12 52 37 PM](https://user-images.githubusercontent.com/95102899/206565222-bbe47b67-9091-4817-a01c-54f6c6160f9d.png)

_______________________________________________________________________________________________________________________________________________________



**2. What range of week numbers are missing from the dataset?**

```sql
WITH gen_series_cte AS (
  SELECT
    GENERATE_SERIES (1, 52) AS week_number
  FROM
    clean_weekly_sales
)
SELECT
  DISTINCT g.week_number
FROM
  gen_series_cte g
  LEFT OUTER JOIN clean_weekly_sales c ON g.week_number = c.week_number
WHERE
  c.week_number IS NULL
ORDER BY
  1 --- we have 28 rows returned - meaning that we are missing 28 weeks
```

![Screen Shot 2022-12-08 at 12 58 05 PM](https://user-images.githubusercontent.com/95102899/206566162-6de50ce0-2ff7-4b94-9b88-5f3ec44e0f8b.png)

_______________________________________________________________________________________________________________________________________________________


**3. How many total transactions were there for each year in the dataset?**

```sql
SELECT 
calendar_year, 
SUM (transactions) AS total_transactions
FROM clean_weekly_sales
GROUP BY calendar_year
ORDER BY calendar_year
```

![Screen Shot 2022-12-08 at 1 09 49 PM](https://user-images.githubusercontent.com/95102899/206568068-82735414-b7b7-485e-86e1-1f8c44b14ed6.png)

_______________________________________________________________________________________________________________________________________________________


**4. What is the total sales for each region for each month?**

```sql
SELECT region, month_number, 
SUM (sales) AS total_sales
FROM clean_weekly_sales
GROUP BY region, month_number
ORDER BY region, month_number
```

![Screen Shot 2022-12-08 at 1 13 02 PM](https://user-images.githubusercontent.com/95102899/206568594-3d71aa02-cfa1-4a19-89e8-afbd1242cc19.png)


_______________________________________________________________________________________________________________________________________________________


**5. What is the total count of transactions for each platform?**

```sql
SELECT 
platform, 
SUM (transactions) AS total_transactions
FROM clean_weekly_sales
GROUP BY platform
ORDER BY platform
```

![Screen Shot 2022-12-08 at 1 17 24 PM](https://user-images.githubusercontent.com/95102899/206569459-423bd3d0-0aeb-40ad-84a5-0c96dddda7e7.png)



_______________________________________________________________________________________________________________________________________________________


**6. What is the percentage of sales for Retail vs Shopify for each month?**

```sql
WITH cte AS (
SELECT 
calendar_year, month_number,platform,
SUM (sales) AS monthly_sales 
FROM clean_weekly_sales
GROUP BY calendar_year,month_number, platform
ORDER BY calendar_year,month_number, platform)

SELECT 
  calendar_year, 
  month_number, 
  ROUND(100 * MAX 
    (CASE WHEN platform = 'Retail' THEN monthly_sales ELSE NULL END) / 
      SUM(monthly_sales),2) AS retail_percentage,
  ROUND(100 * MAX 
    (CASE WHEN platform = 'Shopify' THEN monthly_sales ELSE NULL END) / 
      SUM(monthly_sales),2) AS shopify_percentage
  FROM cte
  GROUP BY calendar_year, month_number
  ORDER BY calendar_year, month_number;
 ```
 
 ![Screen Shot 2022-12-08 at 1 19 23 PM](https://user-images.githubusercontent.com/95102899/206569811-c105c171-123d-4adf-9202-62ee45a71485.png)



_______________________________________________________________________________________________________________________________________________________


**7. What is the percentage of sales by demographic for each year in the dataset?**

```sql
WITH cte AS (
SELECT 
calendar_year, demographic,
SUM (sales) AS yearly_sales 
FROM clean_weekly_sales
GROUP BY calendar_year, demographic
)
  
  SELECT 
  calendar_year, 
 
  ROUND(100 * MAX 
    (CASE WHEN demographic = 'Couples' THEN yearly_sales ELSE NULL END) / 
      SUM(yearly_sales),2) AS couple_percentage,
  ROUND(100 * MAX 
    (CASE WHEN demographic = 'Families' THEN yearly_sales ELSE NULL END) / 
      SUM(yearly_sales),2) AS families_percentage,
  ROUND (100* MAX
    (CASE WHEN demographic = 'Unknown' THEN yearly_sales ELSE NULL END)/
      SUM(yearly_sales),2) AS unknown_demographic_percentage
  FROM cte
  GROUP BY calendar_year 
  ORDER BY calendar_year;
```

![Screen Shot 2022-12-08 at 1 20 39 PM](https://user-images.githubusercontent.com/95102899/206569982-0875d5a9-3c40-47ba-95d8-8e94b24f23f8.png)

_______________________________________________________________________________________________________________________________________________________

**8. Which age_band and demographic values contribute the most to Retail sales?**

```sql
SELECT age_band, demographic, 
SUM (sales),
ROUND(100 * SUM(sales)::NUMERIC / SUM(SUM(sales)) OVER (),2) AS contribution_percentage
FROM clean_weekly_sales
WHERE platform = 'Retail'
GROUP BY age_band, demographic
ORDER BY 3 DESC
```
![Screen Shot 2022-12-08 at 1 22 20 PM](https://user-images.githubusercontent.com/95102899/206570168-f0224fbc-d247-405b-9e1d-133f8958de46.png)

  

_______________________________________________________________________________________________________________________________________________________


**9. Can we use the avg_transaction column to find the average transaction size for each year for Retail vs Shopify? If not - how would you calculate it instead?**

```sql
SELECT calendar_year, platform, ROUND(AVG(avg_transactions), 0) AS avg_transactions_gen, 
SUM(sales) / sum(transactions) AS avg_transaction_group
FROM clean_weekly_sales
GROUP BY platform, calendar_year
ORDER BY calendar_year
```
![Screen Shot 2022-12-08 at 1 23 04 PM](https://user-images.githubusercontent.com/95102899/206570297-9864eb45-765f-4371-b2d4-5952d6adcf27.png)

_______________________________________________________________________________________________________________________________________________________
_______________________________________________________________________________________________________________________________________________________




### C. Before & After Analysis 

#### Assignement 
This technique is usually used when we inspect an important event and want to inspect the impact before and after a certain point in time.

Taking the `week_date` value of `2020-06-15` as the baseline week where the Data Mart sustainable packaging changes came into effect. We would include all `week_date` values for `2020-06-15` as the start of the period after the change and the previous week_date values would be before.

Using this analysis approach - answer the following questions:
1. What is the total sales for the 4 weeks before and after `2020-06-15`? What is the growth or reduction rate in actual values and percentage of sales?
2. What about the entire 12 weeks before and after?
3. How do the sale metrics for these 2 periods before and after compare with the previous years in 2018 and 2019?


**1. What is the total sales for the 4 weeks before and after `2020-06-15`? What is the growth or reduction rate in actual values and percentage of sales?**

First we need to figure out what week number is June 15 2020:

```sql
SELECT DISTINCT week_number
FROM clean_weekly_sales
WHERE week_date = '2020-06-15'
```

| week_number | 
| ------- | 
| 25 | 


```sql
WITH cte1 AS (
SELECT week_number, week_date, 
SUM(sales::NUMERIC) AS total_sales
FROM clean_weekly_sales
WHERE (calendar_year = 2020) AND
(week_number BETWEEN 21 AND 28)
GROUP BY week_date, week_number),

cte2 AS (
SELECT 
SUM (CASE WHEN week_number BETWEEN 21 AND 24 THEN total_sales::NUMERIC END) AS before_sales,
SUM (CASE WHEN week_number BETWEEN 25 AND 28 THEN total_sales::NUMERIC END) AS after_sales
FROM cte1 
)

SELECT *,
after_sales - before_sales AS sales_diff,
ROUND((100 * (after_sales - before_sales)/before_sales),2) percentage
FROM cte2
```

![Screen Shot 2022-12-08 at 1 32 54 PM](https://user-images.githubusercontent.com/95102899/206571997-62e9f6ef-fbe2-43a5-8c22-11c57f102e54.png)


As a result, we got sales result before the new packaging was introduced, sales result after the packaging was introduced, the difference between them in dollars and percentage. As we see, sales were better before the packaging was changed. 

**2. What about the entire 12 weeks before and after?**

```sql
WITH cte1 AS (
SELECT week_number, week_date, 
SUM(sales::NUMERIC) AS total_sales
FROM clean_weekly_sales
WHERE (calendar_year = 2020) AND
(week_number BETWEEN 21 AND 28)
GROUP BY week_date, week_number),

cte2 AS (
SELECT 
SUM (CASE WHEN week_number BETWEEN 21 AND 24 THEN total_sales::NUMERIC END) AS before_sales,
SUM (CASE WHEN week_number BETWEEN 25 AND 28 THEN total_sales::NUMERIC END) AS after_sales
FROM cte1 
)

SELECT *,
after_sales - before_sales AS sales_diff,
ROUND((100 * (after_sales - before_sales)/before_sales),2) percentage
FROM cte2
```
![Screen Shot 2022-12-08 at 1 35 04 PM](https://user-images.githubusercontent.com/95102899/206572385-7b7e5329-ec41-4ee0-be0e-96983a3ed3d5.png)

As we see in the table, sales are still worse 12 weeks after the packaging was changed.

**3. How do the sale metrics for these 2 periods before and after compare with the previous years in 2018 and 2019?**

Here, we'll introduce ``calendar_year`` in the GROUP BY function.

```sql
---4 weeks before and after for 2018, 2019, 2020
WITH cte1 AS (
SELECT week_number, week_date, calendar_year,
SUM(sales) AS total_sales
FROM clean_weekly_sales
WHERE  
week_number BETWEEN 21 AND 28
GROUP BY calendar_year, week_date, week_number),

cte2 AS (
SELECT calendar_year,
SUM (CASE WHEN week_number BETWEEN 21 AND 24 THEN total_sales END) AS before_sales,
SUM (CASE WHEN week_number BETWEEN 25 AND 28 THEN total_sales END) AS after_sales
FROM cte1 
GROUP BY calendar_year
)

SELECT *,
after_sales - before_sales AS sales_diff,
ROUND((100 * (after_sales - before_sales)/before_sales),2) percentage
FROM cte2
```

![Screen Shot 2022-12-08 at 1 37 27 PM](https://user-images.githubusercontent.com/95102899/206572881-f6e1b64d-b49f-4679-b0c1-00e84d146284.png)

---12 weeks before and after for 2018, 2019, 2020

```sql 
WITH cte1 AS (
SELECT week_number, week_date, calendar_year,
SUM(sales) AS total_sales
FROM clean_weekly_sales
WHERE  
week_number BETWEEN 13 AND 36
GROUP BY calendar_year, week_date, week_number),

cte2 AS (
SELECT calendar_year,
SUM (CASE WHEN week_number BETWEEN 13 AND 24 THEN total_sales END) AS before_sales,
SUM (CASE WHEN week_number BETWEEN 25 AND 36 THEN total_sales END) AS after_sales
FROM cte1 
GROUP BY calendar_year
)

SELECT *,
after_sales - before_sales AS sales_diff,
ROUND((100 * (after_sales - before_sales)/before_sales),2) percentage
FROM cte2
```

![Screen Shot 2022-12-08 at 1 38 56 PM](https://user-images.githubusercontent.com/95102899/206572967-87d89c2a-55d0-43ee-9393-ea58732d06e9.png)



