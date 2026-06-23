# 👨‍⚕️ Global Health Metrics Analysis Pipeline

## 🔎 Overview
This repository contains an Apache Zeppelin notebook that uses PySpark and Spark SQL to ingest, clean, transform, and analyze global healthcare data. The pipeline processes a raw World Bank dataset, normalizes complex schemas, imputes missing values using window functions, and performs exploratory data analysis (EDA) on key health indicators.

Evaluating public health outcomes exclusively through financial expenditure leads inconclusive results. This project investigates healthcare efficiency by analyzing how economic inputs (GDP per capita) and infrastructure availability (hospital beds) impact real-world human outcomes (life expentancy, infant mortality).

## 🌉 Architecture & Data Flow

* **Data Ingestion**: Reads raw CSV healthcare data from an HDFS cluster (`hdfs:///user/maria_dev/healthcare/health.csv`).
* **Schema Standardization**: Cleans raw column headers (e.g., removing `[YR2000]` artifacts) and replaces string representations of nulls (`..`) with native Spark Nulls.
* **Reshaping (Unpivot & Pivot)**: Uses Spark's `stack` expression to melt wide-format year columns into long-format rows, then pivots specific health series codes into dedicated columns.
* **Missing Value Imputation**: Uses PySpark Window functions partitioned by country to calculate historical averages, conditionally imputing missing data (e.g., hospital beds per 1k).
* **Data Warehouse Persistence (Apache Hive)**: Physically writes and materializes the clean analytical dataset into the managed Hadoop ecosystem as a persistent Hive table (`global_health_metrics`) using an overwrite execution mode. Includes a  `try-except` fallback block to register an in-memory temporary view if warehouse clusters are unreachable.
* **Exploratory Data Analysis**: Executes multi-dimensional analytics natively through Spark SQL against the managed warehouse view.
  
## 🔑 Key Metrics Tracked
The final structured dataset tracks the following indicators across multiple countries and years:
| Field Name                    | Data Type | Description                                               |
|-------------------------------|-----------|-----------------------------------------------------------|
| country_name                  | String    | Common geographic name of the country or region           |
| country_code                  | String    | 3-letter ISO country identifier                           |
| Year                          | Integer   | Calendar year of observation (2000–2024)                  |
| health_expenditure_gdp        | Float     | Total health expenditure as a % of Gross Domestic Product |
| health_expenditure_per_capita | Float     | Health spending per person in USD                         |
| hospital_beds_per_1k          | Float     | Hospital beds available per 1,000 citizens                |
| life_expectancy               | Float     | Average life expectancy at birth in years                 |
| infant_mortality              | Float     | Infant mortality rate per 1,000 live births               |
| population_above_65_pct       | Float     | Percentage of total population aged 65 and older          |

### Data Limitations
* The raw dataset records null data as a string character (..), which prevents numerical operations
* Data like `hospital_beds_per_1k` are irregularly reported by certain countries like Singapore, resulting in incomplete data

## Preprocessing and Data Cleaning
* Extracted year text from nested column definitions (e.g., matching "2000 [YR2000]" and casting to a clean "2000" header name)
* Replaced string instances of `".."` with actual PySpark `Null` identifiers, forcing the data columns into numeric `FloatType` blocks
* Used PySpark SQL's `stack` expression to transform wide horizontal columns (representing years 2000–2024) into a vertically scalable relational layout containing standardized `Year` and `Value` variables
* Rotated the unique `series_code` categories up into independent columns, ensuring that every singular row maps uniquely to a country-year observation window
* Addressed sparse reporting of `hospital_beds_per_1k` by creating an analytical window partitioned strictly across geographical codes:
  ```python
  country_window = Window.partitionBy("country_code")
  country_mean_beds = F.avg("hospital_beds_per_1k").over(country_window)
  cleaned_pipeline_df = final_structured_df.withColumn(
    "hospital_beds_per_1k", F.coalesce(F.col("hospital_beds_per_1k"), country_mean_beds)
  )
  ```
* Applied a `.cache()` step to register the final dataframe directly in the cluster's memory, ensuring sub-second response times for downstream visual querying

## Results & Analysis
**Life expectancy trend by country**
<img width="1656" height="308" alt="image" src="https://github.com/user-attachments/assets/71a7cdf4-0d91-4f0a-a742-a28196a659cf" />

**Hospital beds available per 1000 citizens by country**
<img width="1656" height="306" alt="image" src="https://github.com/user-attachments/assets/b817cfb2-62c4-45b1-8508-10ce51fb474c" />

**Countries with the highest infant mortality rate**
<img width="1655" height="336" alt="image" src="https://github.com/user-attachments/assets/12e31a51-5a45-413d-befa-cb58baaf01c6" />


| Country             | Average Healthcare Spending (GDP) Percent | Average Life Expectancy | Net Life Expectancy Growth |
|---------------------|-------------------------------------------|-------------------------|----------------------------|
| Japan               | 9.53                                      | 83.1                    | 3.5                        |
| Singapore           | 3.76                                      | 81.5                    | 5.6                        |
| United States       | 15.7                                      | 78.0                    | 2.6                        |
| Malaysia            | 3.38                                      | 75.0                    | 4.1                        |
| Thailand            | 3.7                                       | 74.9                    | 6.4                        |
| Viet Nam            | 4.58                                      | 73.8                    | 2.6                        |
| Indonesia           | 2.71                                      | 68.6                    | 5.8                        |

* Higher spending does not necessarily guarantee better health outcomes. For example, the United States spends 15.7% of their GDP on healthcare but the average life expectancy is only 78.0 years while having a growth of only 2.6 years.
* Japan has the highest average life expectancy of 83.1 years while maintaining a moderate spending of 9.53% of their GDP. Singapore has the second highest average life expectancy at 81.5 years, while spending only 3.76% of their GDP and also having a larger life expectancy growth of 5.6. Both of these nations have efficient infrastructure resource allocation.
* Malaysia has a highly convservative spending of 3.38% of their GDP while maintaining a moderate average life expectancy of 75.0 years. However, Thailand, which has a similar spending and average life expectancy, is able to achieve a greater net life expectancy growth over Malaysia at 6.4 years.
* Malaysia’s infrastructure trends shows a steady, reduction on facilities, with local hospital beds per 1,000 citizens declining from 2.05 in the year 2000 down to approximately 1.92 by the year 2024.

### SQL Queries
The transformed records are registered as a temporary Spark SQL view: `cleaned_pipeline_df.createOrReplaceTempView("global_health_metrics")`

* Query 1: Country Profile
  This query pulls the localized yearly progression of investments and corresponding life expectancies. The Zeppelin notebook uses dynamic forms (`${country=MYS}`) to easily filter outputs directly from the user interface.
  ```sql
  SELECT Year, country_name, health_expenditure_gdp, life_expectancy, hospital_beds_per_1k
  FROM global_health_metrics
  WHERE country_code = '${country=MYS}'
  ORDER BY Year ASC;
  ```

* Query 2: Healthcare Investment Insights
  ```sql
  SELECT 
    country_name AS Country,
    ROUND(AVG(health_expenditure_gdp), 2) AS Avg_Healthcare_Spending_GDP_Pct,
    ROUND(AVG(life_expectancy), 1) AS Avg_Life_Expectancy_Years
  FROM 
      global_health_metrics
  WHERE 
      country_code != 'EAS'  -- Crucial filter: removes the macro aggregate to keep comparisons strictly peer-to-peer
  GROUP BY 
      country_name
  ORDER BY 
      Avg_Life_Expectancy_Years DESC
  ```

### Recommendations
* Implementing an efficient healthcare system provides the best outcome for human health, as validated in the results by nations like Singapore. Although a country may spend a large portion of its GDP on healthcare, the returns are diminishing if the underlying framework is not sufficient or flawed.
* Thailand is a notable nation, in the sense that they achieved a 6.4 year life expectancy growth with a relatively low percentage of GDP spending. Developing nations should look to Thailand's healthcare policies to identify reproducible strategies.
* Life expectancy in Malaysia is steadily rising. However, there is a decline in the number of hospital beds per 1000 citizens, indicating that better, more strategic investment is needed. As the population ages, unresolved infrastructure issues will heavily impact long-term healthcare systems.

## Conclusion


## ⚙ Prerequisites
* Apache Spark (2.x or 3.x).
* Apache Zeppelin.
* Apache Hive Data Warehouse.
* Python 3.x with PySpark.

## 💾 Usage
1. Import the `final_assignment.json` file into your Apache Zeppelin environment.
2. Ensure the source data is placed in the designated HDFS path.
3. Run the notebook paragraphs sequentially. 
4. Use Zeppelin's built-in charting features to visualize the outputs of the Spark SQL paragraphs. Dynamic variables (like `${country=MYS}`) can be adjusted directly in the UI to filter different regions.
