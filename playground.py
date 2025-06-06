import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, TimestampType
from datetime import datetime

data_schema = StructType([
    StructField("product_id", IntegerType(), False),  # Not nullable
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("in_stock", BooleanType(), True),
    StructField("last_updated", TimestampType(), True)
])

dummy_data = [
    (101, "Laptop Pro X", "Electronics", 1200, True, datetime(2023, 10, 26, 10, 0, 0)),
    (102, "Mechanical Keyboard", "Electronics", 150, True, datetime(2023, 10, 26, 10, 30, 0)),
    (103, "Ergonomic Chair", "Furniture", 450, False, datetime(2023, 10, 25, 15, 0, 0)),
    (104, "Wireless Mouse", "Electronics", 35, True, datetime(2023, 10, 26, 11, 0, 0)),
    (105, "Coffee Maker", "Kitchen", 80, True, datetime(2023, 10, 24, 9, 0, 0)),
    (106, "Smartwatch", "Wearables", 299, True, datetime(2023, 10, 26, 12, 0, 0)),
    (107, "Desk Lamp", "Furniture", 40, True, datetime(2023, 10, 25, 16, 0, 0)),
    (108, "USB-C Hub", "Electronics", 25, False, datetime(2023, 10, 26, 13, 0, 0))
]

df = spark.createDataFrame(data=dummy_data, schema=data_schema)

catalog_name = "dbrcat_lab_di_0"
schema_name = "default"
table_name = "dummy_products"
full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable(full_table_name)
