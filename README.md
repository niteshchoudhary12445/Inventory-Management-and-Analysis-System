# Inventory-Management-and-Analysis-System
This project provides a Python-based system for ingesting, aggregating, and analyzing inventory data from CSV files into a SQL Server database. It includes scripts for data ingestion, aggregation, and a Jupyter Notebook for vendor performance analysis.
Project Structure

aggregate.py: Aggregates data from vendor_invoice, purchases, purchase_prices, and sales tables into a final_summary table with metrics like Gross Profit, Profit Margin, Stock Turnover, and Sales-to-Purchase Ratio.
ingestion.py: Ingests CSV files from specified folders into corresponding SQL Server tables, then triggers aggregation.
utils.py: Contains shared utilities for logging, database connections, table management, and file discovery.
Vendor_Performance_Analysis.ipynb: Jupyter Notebook analyzing vendor performance using the final_summary table, including statistical tests and visualizations like profit margin distributions.
data/: Folder for input CSV files (not included in the repository).
logs/: Folder for log files generated during ingestion and aggregation.

Prerequisites

Python 3.12+
SQL Server (e.g., SQL Server Express) with a database named inventory_db.
ODBC Driver 17 for SQL Server installed.
Python packages:pip install pandas pyodbc sqlalchemy matplotlib seaborn scipy


Jupyter Notebook for running the analysis:pip install jupyter



Setup

Clone the Repository:
git clone <repository-url>
cd <repository-folder>


Configure SQL Server:

Ensure SQL Server is running on DESKTOP-5RU5DHA\SQLEXPRESS (or update the connection string in utils.py under IngestionConfig.CONN_STRING).
Create a database named inventory_db.


Prepare Data:

Place CSV files in the data/ folder. Expected files include columns for:
vendor_invoice: VendorNumber, Freight
purchases: VendorNumber, VendorName, Brand, Description, PurchasePrice, Quantity, Dollars
purchase_prices: Brand, Price, Volume
sales: VendorNo, Brand, SalesQuantity, SalesDollars, SalesPrice, ExciseTax




Install Dependencies:
pip install -r requirements.txt

(Create a requirements.txt with the listed packages if needed.)


Usage

Run Ingestion and Aggregation:
python ingestion.py


This script ingests CSV files from the data/ folder into SQL Server tables and runs the aggregation to populate the final_summary table.
Logs are saved to logs/ingestion.log and logs/aggregate.log.


Run Analysis:
jupyter notebook Vendor_Performance_Analysis.ipynb


Open the notebook in Jupyter and execute the cells to analyze vendor performance.
The notebook generates visualizations (e.g., profit margin histograms) and performs statistical tests (e.g., t-test for profit margins).
Exports the final_summary table to data/final_summary.csv.



Key Features

Data Ingestion: Streams CSV files into SQL Server in chunks to handle large datasets efficiently.
Data Aggregation: Combines data from multiple tables to compute key performance metrics (e.g., Gross Profit, Profit Margin).
Vendor Analysis: Compares high- and low-performing vendors based on sales volume and profit margins, with confidence intervals and statistical significance testing.
Logging: Detailed logs for ingestion and aggregation processes.
Extensibility: Modular design allows for easy addition of new data sources or metrics.

Notes

Ensure the SQL Server connection string in utils.py matches your environment.
The Vendor_Performance_Analysis.ipynb assumes the final_summary table exists. Run ingestion.py first to populate it.
The notebook includes a t-test to compare profit margins between high- and low-performing vendors, with results indicating significant differences.

Example Output

Database: The final_summary table contains columns like VendorNumber, VendorName, Brand, Description, PurchasePrice, TotalSalesDollars, GrossProfit, ProfitMargin, etc.
CSV Export: data/final_summary.csv contains the aggregated data for further analysis.
Visualizations: The notebook generates histograms showing profit margin distributions for high- and low-performing vendors.

Troubleshooting

Connection Errors: Verify the SQL Server instance is running and the connection string is correct.
Empty Files: The ingestion script skips empty CSV files and logs a warning.
Missing Columns: Ensure CSV files have the expected columns, or the ingestion may fail.
Performance: Adjust CHUNK_SIZE in utils.py for large datasets to balance memory usage and speed.

License
This project is licensed under the MIT License.
