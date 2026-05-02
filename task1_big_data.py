# Import necessary libraries 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, sum, avg, count 

# 1. Initialize Spark Session (Demonstrating Scalability) 
# The .builder allows Spark to scale from a single laptop to a cluster of machines
spark = SparkSession.builder \
    .appName("BigDataRetailAnalysis") \
    .getOrCreate() 

print("Spark Session Initialized Successfully!") 

# 2. Load the Large Dataset 
# Use 'inferSchema=True' to let Spark automatically detect data types
# Ensure 'retail_data.csv' is in the same folder as this script
try: 
    df = spark.read.csv("retail_data.csv", header=True, inferSchema=True) 
    print("Dataset Loaded. Total Records:", df.count()) 
except Exception as e: 
    print("Error loading data: Ensure 'retail_data.csv' is in the folder.") 

# 3. Data Cleaning (Handling Big Data challenges) 
# Removing null values to ensure analysis accuracy 
df_cleaned = df.dropna() 

# 4. Big Data Processing: Insight 1 - Total Sales by Category 
# We use .groupBy which is optimized for distributed computing 
category_sales = df_cleaned.groupBy("Category") \
    .agg(sum("Total_Price").alias("Total_Revenue")) \
    .orderBy(col("Total_Revenue").desc()) 

print("--- Total Revenue by Category ---") 
category_sales.show() 

# 5. Big Data Processing: Insight 2 - Average Rating by Product 
# This demonstrates Spark's ability to aggregate over millions of rows quickly 
product_performance = df_cleaned.groupBy("ProductID") \
    .agg(avg("Rating").alias("Average_Rating"), count("ProductID").alias("Review_Count")) \
    .filter(col("Review_Count") > 100) \
    .orderBy(col("Average_Rating").desc()) 

print("--- Top Rated Products (with over 100 reviews) ---") 
product_performance.show(10) 

# Stop the Spark Session 
spark.stop()