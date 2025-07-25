from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, count, lit

# Initialize SparkSession
spark = SparkSession.builder.appName("DropNullColumns").getOrCreate()

# Create a sample DataFrame with null values
data = [
    ("Alice", 1, 100, "A"),
    ("Bob", 2, None, "B"),
    ("Charlie", None, 300, "C"),
    ("David", 4, 400, None),
    ("Eve", None, None, None),
    ("Frank", 6, 600, "F")
]
columns = ["Name", "ID", "Score", "Category"]
df = spark.createDataFrame(data, columns)
df.show()

# Method 1: Drop columns that have ANY null values
# -------------------------------------------------

# Calculate null counts for each column
null_counts = df.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns])
null_counts_dict = null_counts.first().asDict()

# Identify columns to drop (where null count > 0)
cols_to_drop_any_null = [k for k, v in null_counts_dict.items() if v > 0]

print(f"Columns with any null values to drop: {cols_to_drop_any_null}")

# Drop the identified columns
df_dropped_any_null = df.drop(*cols_to_drop_any_null)
print("DataFrame after dropping columns with any null values:")
df_dropped_any_null.show()

# Method 2: Drop columns that have ALL null values
# -------------------------------------------------

total_rows = df.count()
print(f"Total rows in DataFrame: {total_rows}")

# Calculate null counts for each column
null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()

# Identify columns to drop (where null count equals total_rows)
cols_to_drop_all_null = [c for c, count in null_counts.items() if count == total_rows]

print(f"Columns with all null values to drop: {cols_to_drop_all_null}")

# Drop the identified columns
df_dropped_all_null = df.drop(*cols_to_drop_all_null)
print("DataFrame after dropping columns with all null values:")
df_dropped_all_null.show()

# Method 3: Drop columns based on a threshold of null values (e.g., > 50% nulls)
# -----------------------------------------------------------------------------

null_percentage_threshold = 0.5 # 50%

cols_to_drop_threshold = []
for c in df.columns:
    null_count = df.filter(col(c).isNull()).count()
    if total_rows > 0 and (null_count / total_rows) > null_percentage_threshold:
        cols_to_drop_threshold.append(c)

print(f"Columns with more than {null_percentage_threshold*100}% null values to drop: {cols_to_drop_threshold}")

df_dropped_threshold = df.drop(*cols_to_drop_threshold)
print("DataFrame after dropping columns based on null percentage threshold:")
df_dropped_threshold.show()

# Stop SparkSession
spark.stop()