# sql-data-warehouse-project
Building a modern data warehouse, including ETL processes, data modeling and analytics
Data Warehouse Project (MySQL)

📌 Project Description

This project demonstrates the creation of a Data Warehouse (DWH) using MySQL.
The goal is to design a robust data architecture, implement ETL processes, and build a star schema to enable business analytics.

🏗 Data Architecture

The project follows the medallion architecture approach:
	•	Bronze — raw data ingestion from CSV files.
	•	Silver — data cleaning, standardization, and transformation.
	•	Gold — creation of business-ready fact and dimension tables for analytics and reporting.

⚙️ Technologies Used
	•	Database: MySQL 8+
	•	Language: SQL
	•	Tools: MySQL Workbench, Git, CSV

🚀 How to Run
	1.	Clone the repository.
	2.	Create a MySQL database using the init_database.sql script.
	3.	Load the raw data using the Bronze scripts.
	4.	Run the Silver scripts to clean and transform the data.
	5.	Build the Gold layer (star schema) using the provided scripts.
	6.	Run the data quality checks from the tests folder.
	7.	Execute analytical queries from the reports folder.

📊 Implemented Features
	•	Data warehouse design in MySQL.
	•	ETL process split into Bronze, Silver, and Gold layers.
	•	Star schema with fact and dimension tables.
	•	Query optimization and indexing.
	•	Business analytics: sales trends, customer behavior, and key metrics.

🧠 Key Learnings
	•	Data warehouse architecture design.
	•	Using SQL for data transformation and analytics.
	•	Building star schemas for reporting.
	•	Data quality validation and query optimization in MySQL.

 💼 Business Use Cases
	•	Revenue analysis: Identify top customers, products, and regions.
	•	Customer behavior tracking: Measure repeat purchases and customer lifetime value (CLV).
	•	Trend analysis: Monitor monthly revenue growth and seasonality.
	•	Product performance: Evaluate best-selling product categories.
	•	KPI reporting: Build a foundation for executive dashboards and business insights.
