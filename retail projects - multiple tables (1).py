dbutils.fs.mount (   
    source = "wasbs://retail@gunastroageacccount.blob.core.windows.net/",
    mount_point = "/mnt/retail",
    extra_configs = {"fs.azure.account.key.gunastroageacccount.blob.core.windows.net": "OBgKIo6TqIk+rvDceC5/Q3SDKnvI2YyeK9CEK+i/G1AJ+RlE/ec3DByIsxsjMvwLihzuz2DhDpBS+AStiUDDKA=="}
    )

dbutils.fs.ls('/mnt/retail/bronze /')
df_transactions =spark.read.parquet('/mnt/retail/bronze /transactions/')
df_products =spark.read.parquet('/mnt/retail/bronze /products /')
df_stores =spark.read.parquet('/mnt/retail/bronze /stores/')
df_customers =spark.read.parquet('/mnt/retail/bronze /customers/manish040596/azure-data-engineer---multi-source/refs/heads/main/')
display(df_customers)

display(df_transactions)
from pyspark.sql.functions import *

df_transactions = df_transactions.select(
    col("transaction_id").cast("integer"),
    col("customer_id").cast("integer"),
    col("store_id").cast("integer"),
    col("product_id").cast("integer"),
    col("quantity").cast("integer"),
    col("transaction_date").cast("date")
   
)
    
display(df_products)
from pyspark.sql.functions import  *
df_products = df_products.select(
    col('product_id').cast('int'),
    col('product_name').cast('string'),
    col('category').cast('string'),
    col('price').cast('double')
)
display(df_products)

from pyspark.sql.functions import *
df_stores=df_stores.select(
    col('store_id').cast('int'),
    col('store_name').cast('string'),
    col('location').cast('string')
)
display(df_stores)
display(df_customers)

from pyspark.sql.functions import *
df_customers=df_customers.select(
    col('customer_id').cast('int'),
    col('first_name').cast('string'),
    col('last_name').cast('string'),
    col('email').cast('string'),
    col('phone').cast('string'),
    col('city').cast('string'),
    col('registration_date').cast('date')
)
display(df_customers)

df_silver = df_transactions \
    .join(df_products,'product_id') \
    .join(df_stores,'store_id') \
    .join(df_customers,'customer_id') \
.withColumn('totalamount',col('quantity')*col('price'))
display(df_silver)
silver_path = "/mnt/retail/silver"
df_silver.write.mode('overwrite').format('delta').save(silver_path)

spark.sql(f"""
        CREATE TABLE retail_silver_cleaned
        USING DELTA
        LOCATION "/mnt/retail/silver"
        """
)
%sql select * from retail_silver_cleaned
silver_df = spark.read.format('delta').load("/mnt/retail/silver")
display(silver_df)
from pyspark.sql.functions import *
gold_df=silver_df.groupBy(
    'transaction_date',
    'product_id','product_name','category',
    'store_id','store_name','location'
    ).agg(
        sum('quantity').alias('total_quantity_sold'),
        sum('totalamount').alias('total_sales_amount'),
        countDistinct('transaction_id').alias('number_of_transactions'),
        avg('totalamount').alias('average_transaction_value')
    )
display(gold_df)
gold_path = "/mnt/retail/gold"
gold_df.write.mode('overwrite').format('delta').save(gold_path)
display(gold_df)
spark.sql(f"""
          CREATE TABLE retail_gold_summary
          USING DELTA
          LOCATION "/mnt/retail/gold"
       
        """)
%sql select * from retail_gold_summary
