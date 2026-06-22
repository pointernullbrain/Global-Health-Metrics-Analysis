# 👨‍⚕️ Global Health Metrics Analysis Pipeline

## 🔎 Overview
This repository contains an Apache Zeppelin notebook that uses PySpark and Spark SQL to ingest, clean, transform, and analyze global healthcare data. The pipeline processes a raw World Bank dataset, normalizes complex schemas, imputes missing values using window functions, and performs exploratory data analysis (EDA) on key health indicators.

## 🌉 Architecture & Data Flow

* **Data Ingestion**: Reads raw CSV healthcare data from an HDFS cluster (`hdfs:///user/maria_dev/healthcare/health.csv`).
* **Schema Standardization**: Cleans raw column headers (e.g., removing `[YR2000]` artifacts) and replaces string representations of nulls (`..`) with native Spark Nulls.
* **Reshaping (Unpivot & Pivot)**: Uses Spark's `stack` expression to melt wide-format year columns into long-format rows, then pivots specific health series codes into dedicated analytical columns.
* **Missing Value Imputation**: Employs PySpark Window functions partitioned by country to calculate historical averages, conditionally imputing missing data (e.g., hospital beds per 1k).
* **In-Memory Caching**: Caches the cleaned DataFrame to optimize downstream SQL queries and visualizations.
* **Exploratory Data Analysis**: Registers a temporary SQL view (`global_health_metrics`) to query historical trends for specific countries (like Malaysia) and aggregate global rankings.

## 🔑 Key Metrics Tracked
The final structured dataset tracks the following indicators across multiple countries and years:
* `health_expenditure_gdp`: Healthcare spending as a percentage of GDP.
* `health_expenditure_per_capita`: Healthcare spending per capita (USD).
* `hospital_beds_per_1k`: Number of hospital beds per 1,000 people.
* `life_expectancy`: Life expectancy at birth.
* `infant_mortality`: Infant mortality rate.
* `population_above_65_pct`: Percentage of the population aged 65 and above.

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
