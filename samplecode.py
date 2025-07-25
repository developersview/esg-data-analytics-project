# ESG_ETL_PIPELINE - DATABRICKS FREE EDITION
# Modular ETL with Bronze–Silver–Gold Architecture for 6 Climate TRACE files

# ------------------------------
# 1. Extract Task: Bronze Layer
# ------------------------------
# Filename: extract_bronze.py / notebook

from pyspark.sql import SparkSession
from datetime import datetime
import logging
import pyspark.sql.functions as F

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Extract")

spark = SparkSession.builder.appName("ESG Bronze Extract").getOrCreate()

# List of real filenames to extract
file_parts = [
    "electricity-generation_country_emissions_v4_4_0.csv",
    "electricity-generation_emissions_sources_confidence_v4_4_0.csv",
    "electricity-generation_emissions_sources_ownership_v4_4_0.csv",
    "electricity-generation_emissions_sources_v4_4_0.csv",
    "heat-plants_country_emissions_v4_4_0.csv",
    "other-energy-use_country_emissions_v4_4_0.csv"
]

for i, file_name in enumerate(file_parts):
    df_raw = spark.read.option("header", True).option("inferSchema", True).csv(f"/dbfs/FileStore/esg_raw/{file_name}")
    df_raw = df_raw.withColumn("ingestion_timestamp", F.current_timestamp())
    df_raw.write.format("delta").mode("overwrite").saveAsTable(f"bronze.{file_name.replace('.csv','').replace('-','_')}_raw")
    logger.info(f"Extracted and saved: {file_name}")

# -------------------------------
# 2. Transform Task: Silver Layer
# -------------------------------
# Filename: transform_silver.py / notebook

from pyspark.sql.functions import col, trim, when

# 1. electricity-generation_country_emissions
try:
    df = spark.table("bronze.electricity_generation_country_emissions_v4_4_0_raw")
    df = (
        df.select([trim(col(c)).alias(c.strip()) for c in df.columns])
        .filter(col("facility_id").isNotNull())
        .filter(col("ghg_emissions_co2e").isNotNull())
        .withColumn("year", col("year").cast("int"))
        .filter(col("year") >= 2015)
        .na.fill({"estimated_generation_gwh": 0, "data_confidence": 1.0})
    )
    df.write.format("delta").mode("overwrite").saveAsTable("silver.electricity_generation_country_emissions_v4_4_0_cleaned")
    logger.info("Transformed: electricity-generation_country_emissions")
except Exception as e:
    logger.error(f"Transform failed: {e}")

# 2. electricity-generation_emissions_sources_confidence
try:
    df = spark.table("bronze.electricity_generation_emissions_sources_confidence_v4_4_0_raw")
    df = df.select([trim(col(c)).alias(c.strip()) for c in df.columns]).dropna(how="all")
    df.write.format("delta").mode("overwrite").saveAsTable("silver.electricity_generation_emissions_sources_confidence_v4_4_0_cleaned")
    logger.info("Transformed: emissions_sources_confidence")
except Exception as e:
    logger.error(f"Transform failed: {e}")

# 3. electricity-generation_emissions_sources_ownership
try:
    df = spark.table("bronze.electricity_generation_emissions_sources_ownership_v4_4_0_raw")
    df = df.select([trim(col(c)).alias(c.strip()) for c in df.columns]).dropna(how="all")
    df.write.format("delta").mode("overwrite").saveAsTable("silver.electricity_generation_emissions_sources_ownership_v4_4_0_cleaned")
    logger.info("Transformed: emissions_sources_ownership")
except Exception as e:
    logger.error(f"Transform failed: {e}")

# 4. electricity-generation_emissions_sources
try:
    df = spark.table("bronze.electricity_generation_emissions_sources_v4_4_0_raw")
    df = df.select([trim(col(c)).alias(c.strip()) for c in df.columns]).dropna(how="all")
    df.write.format("delta").mode("overwrite").saveAsTable("silver.electricity_generation_emissions_sources_v4_4_0_cleaned")
    logger.info("Transformed: emissions_sources")
except Exception as e:
    logger.error(f"Transform failed: {e}")

# 5. heat-plants_country_emissions
try:
    df = spark.table("bronze.heat_plants_country_emissions_v4_4_0_raw")
    df = (
        df.select([trim(col(c)).alias(c.strip()) for c in df.columns])
        .filter(col("facility_id").isNotNull())
        .filter(col("ghg_emissions_co2e").isNotNull())
        .withColumn("year", col("year").cast("int"))
        .filter(col("year") >= 2015)
        .na.fill({"estimated_generation_gwh": 0, "data_confidence": 1.0})
    )
    df.write.format("delta").mode("overwrite").saveAsTable("silver.heat_plants_country_emissions_v4_4_0_cleaned")
    logger.info("Transformed: heat-plants_country_emissions")
except Exception as e:
    logger.error(f"Transform failed: {e}")

# 6. other-energy-use_country_emissions
try:
    df = spark.table("bronze.other_energy_use_country_emissions_v4_4_0_raw")
    df = (
        df.select([trim(col(c)).alias(c.strip()) for c in df.columns])
        .filter(col("facility_id").isNotNull())
        .filter(col("ghg_emissions_co2e").isNotNull())
        .withColumn("year", col("year").cast("int"))
        .filter(col("year") >= 2015)
        .na.fill({"estimated_generation_gwh": 0, "data_confidence": 1.0})
    )
    df.write.format("delta").mode("overwrite").saveAsTable("silver.other_energy_use_country_emissions_v4_4_0_cleaned")
    logger.info("Transformed: other-energy-use_country_emissions")
except Exception as e:
    logger.error(f"Transform failed: {e}")

# --------------------------
# 3. Load Task: Gold Layer
# --------------------------
# Filename: load_gold_kpis.py / notebook

from pyspark.sql.functions import sum, avg, expr, when

# Only calculate KPIs for compatible files
try:
    df = spark.table("silver.electricity_generation_country_emissions_v4_4_0_cleaned")
    df_kpis = df.groupBy("facility_id", "country_name") \
        .agg(
            sum("ghg_emissions_co2e").alias("total_emissions"),
            sum("estimated_generation_gwh").alias("total_generation"),
            avg("data_confidence").alias("avg_confidence")
        ) \
        .withColumn("emissions_intensity", expr("total_emissions / total_generation")) \
        .withColumn("high_risk_flag", when(col("avg_confidence") < 0.5, "Yes").otherwise("No"))

    df_kpis.write.format("delta").mode("overwrite").saveAsTable("gold.electricity_generation_country_emissions_v4_4_0_kpis")
    df_kpis.write.option("header", True).mode("overwrite").csv("/dbfs/FileStore/gold_exports/electricity_generation_country_emissions_v4_4_0_kpis")
    logger.info("Gold KPI generated and exported")
except Exception as e:
    logger.error(f"Gold KPI generation failed: {e}")
