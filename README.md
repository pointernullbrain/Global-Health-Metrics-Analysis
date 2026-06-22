# 👨‍⚕️ Global Health Metrics Analysis Pipeline

## 🔎 Overview
This repository contains an Apache Zeppelin notebook that uses PySpark and Spark SQL to ingest, clean, transform, and analyze global healthcare data. The pipeline processes a raw World Bank dataset, normalizes complex schemas, imputes missing values using window functions, and performs exploratory data analysis (EDA) on key health indicators.

## Problem Statement
Evaluating public health outcomes exclusively through financial expenditure leads inconclusive results. This project investigates healthcare efficiency by analyzing how economic inputs (GDP per capita) and infrastructure availability (hospital beds) impact real-world human outcomes (life expentancy, infant mortality).

## 🌉 Architecture & Data Flow

* **Data Ingestion**: Reads raw CSV healthcare data from an HDFS cluster (`hdfs:///user/maria_dev/healthcare/health.csv`).
* **Schema Standardization**: Cleans raw column headers (e.g., removing `[YR2000]` artifacts) and replaces string representations of nulls (`..`) with native Spark Nulls.
* **Reshaping (Unpivot & Pivot)**: Uses Spark's `stack` expression to melt wide-format year columns into long-format rows, then pivots specific health series codes into dedicated analytical columns.
* **Missing Value Imputation**: Employs PySpark Window functions partitioned by country to calculate historical averages, conditionally imputing missing data (e.g., hospital beds per 1k).
* **In-Memory Caching**: Caches the cleaned DataFrame to optimize downstream SQL queries and visualizations.
* **Exploratory Data Analysis**: Registers a temporary SQL view (`global_health_metrics`) to query historical trends for specific countries (like Malaysia) and aggregate global rankings.

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

## SQL Queries
The transformed records are registered as a temporary Spark SQL view: `cleaned_pipeline_df.createOrReplaceTempView("global_health_metrics")`

* Query 1: Country Profile
  This query pulls the localized yearly progression of investments and corresponding life expectancies. The Zeppelin notebook leverages dynamic forms (`${country=MYS}`) to easily filter outputs directly from the user interface
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
    ROUND(AVG(life_expectancy), 1) AS Avg_Life_Expectancy_Years,
    ROUND(MAX(life_expectancy) - MIN(life_expectancy), 1) AS Net_Life_Expectancy_Growth
  FROM global_health_metrics
  GROUP BY country_name
  ORDER BY Avg_Life_Expectancy_Years DESC;
  ```
## Results (later revise language)
  * Higher spending as a percentage of a nation's GDP does not automatically guarantee superior health outcomes. The United States exhibits a massive average     healthcare spend of 15.7% of its total GDP, yet yields a comparatively lower average life expectancy of 78.0 years alongside a minor net lifetime expansion growth of 2.6 years over the observed era
  * Nations like Japan and Singapore demonstrate highly optimal infrastructure resource allocation. Japan leads the global cohort with an average life expectancy of 83.1 years while maintaining a moderate 9.53% GDP spend profile. Singapore secures an average lifespan of 81.5 years and a major net expansion gain of 5.6 years, while spending an average of just 3.76% of its GDP on health.
  * In Southeast Asia, Malaysia maintains a stable profile, averaging a 75.0-year life expectancy on a highly conservative 3.38% GDP spend profile. However, neighboring Thailand indicates a massive net life expectancy growth of 6.4 years using a nearly identical budget footprint (3.70% of GDP).
  * A granular inspection of Malaysia’s micro-trends shows a steady, marginal compression in structural capacities, with local hospital beds per 1,000 citizens declining from 2.05 in the year 2000 down to approximately 1.92 by the year 2024.

## ⚙ Prerequisites
* Apache Spark (2.x or 3.x)
* Apache Zeppelin
* Hadoop Distributed File System (HDFS)
* Python 3.x with PySpark

## 💾 Usage
1. Import the `final_assignment.json` file into your Apache Zeppelin environment.
2. Ensure the source data is placed in the designated HDFS path.
3. Run the notebook paragraphs sequentially. 
4. Use Zeppelin's built-in charting features to visualize the outputs of the Spark SQL paragraphs. Dynamic form variables (like `${country=MYS}`) can be adjusted directly in the UI to filter different regions.
