from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, sum, avg, count, regexp_replace

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("BigDataRetailAnalysis") \
    .getOrCreate() 

print("Spark Session Initialized Successfully!") 

# 2. Load the Dataset
try: 
    # Using the correct filename you have in your folder
    df = spark.read.csv("retail_data.csv", header=True, inferSchema=True) 
    print("Dataset Loaded. Total Records:", df.count()) 
except Exception as e: 
    print("Error loading data: Ensure 'retail_data.csv' is in the folder.") 

# --- STEP 3: Clean Prices AND Ratings ---
from pyspark.sql.functions import expr, col, regexp_replace

# 1. Clean actual_price
df_price = df.withColumn("price_numeric", 
    expr("try_cast(regexp_replace(actual_price, '[^0-9.]', '') AS FLOAT)"))

# 2. Clean rating (This fixes the '|' error you just got)
df_rating = df_price.withColumn("rating_numeric", 
    expr("try_cast(regexp_replace(rating, '[^0-9.]', '') AS FLOAT)"))

# 3. Clean rating_count
df_final = df_rating.withColumn("count_numeric", 
    expr("try_cast(regexp_replace(rating_count, '[^0-9.]', '') AS INT)"))

# Drop rows that failed any conversion to keep results accurate
df_final = df_final.dropna(subset=["price_numeric", "rating_numeric"])

print("Final Cleaned Records:", df_final.count())

# 4. Processing
category_sales = df_final.groupBy("category") \
    .agg(sum("price_numeric").alias("Total_Revenue")) \
    .orderBy(col("Total_Revenue").desc())

category_sales.show()

# --- STEP 5: Average Rating (Updated with new numeric columns) ---
product_performance = df_final.groupBy("product_id") \
    .agg(
        avg("rating_numeric").alias("Average_Rating"), 
        sum("count_numeric").alias("Total_Reviews")
    ) \
    .filter(col("Total_Reviews") > 5) \
    .orderBy(col("Average_Rating").desc()) 

print("--- Top Rated Products ---") 
product_performance.show(10)

# Stop the Spark Session 
spark.stop()
