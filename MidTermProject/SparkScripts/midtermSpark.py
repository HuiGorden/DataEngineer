from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, FloatType, DateType, StringType, BooleanType, DecimalType
from pyspark.sql.functions import col, sum as SUM, avg, unix_timestamp, to_date, date_format, coalesce, lit, when, count
import json
import argparse

class SparkRunner:

    def __init__(self, spark_name, s3_calendar_location, s3_inventory_csv_location, s3_product_csv_location, s3_sales_csv_location, s3_store_csv_location) -> None:
        self.spark_name = spark_name
        self.s3_calendar_location = s3_calendar_location
        self.s3_inventory_csv_location = s3_inventory_csv_location
        self.s3_product_csv_location = s3_product_csv_location
        self.s3_sales_csv_location = s3_sales_csv_location
        self.s3_store_csv_location = s3_store_csv_location
        self.dateStr = self.s3_calendar_location.split("/")[1]
        self.output_file_url = "s3://hui-mid-term/output/"
        self.spark = self.spark_init(spark_name)

    def spark_init(self, spark_name):
        spark = SparkSession.builder.appName(spark_name).getOrCreate()
        return spark

    def etl(self, spark):
        calendar_schema= [
            StructField("CAL_DT", DateType(), False), \
            StructField("CAL_TYPE_DESC", StringType(), False), \
            StructField("DAY_OF_WK_NUM", IntegerType(), False), \
            StructField("DAY_OF_WK_DESC", StringType(), False), \
            StructField("YR_NUM", IntegerType(), False), \
            StructField("WK_NUM", IntegerType(), False), \
            StructField("YR_WK_NUM", IntegerType(), False), \
            StructField("MNTH_NUM", IntegerType(), False), \
            StructField("YR_MNTH_NUM", IntegerType(), False), \
            StructField("QTR_NUM", IntegerType(), False), \
            StructField("YR_QTR_NUM", IntegerType(), False)
        ]
        calendar_struc = StructType(fields=calendar_schema)
        calendar_dim = spark.read.csv(f's3a://hui-mid-term/{self.s3_calendar_location}', header=True, schema=calendar_struc)

        store_schema= [
            StructField("STORE_KEY", IntegerType(), False), \
            StructField("STORE_NUM", StringType(), False), \
            StructField("STORE_DESC", StringType(), False), \
            StructField("ADDR", StringType(), False), \
            StructField("CITY", StringType(), False), \
            StructField("REGION", StringType(), False), \
            StructField("CNTRY_CD", StringType(), False), \
            StructField("CNTRY_NM", StringType(), False), \
            StructField("POSTAL_ZIP_CD", StringType(), False), \
            StructField("PROV_STATE_CD", StringType(), False), \
            StructField("PROV_STATE_DESC", StringType(), False), \
            StructField("STORE_TYPE_CD", StringType(), False),\
            StructField("STORE_TYPE_DESC", StringType(), False), \
            StructField("FRNCHS_FLG", BooleanType(), False), \
            StructField("STORE_SIZE", DecimalType(), False), \
            StructField("MARKET_KEY", IntegerType(), False),\
            StructField("MARKET_NAME", StringType(), False), \
            StructField("SUBMARKET_KEY", IntegerType(), False), \
            StructField("SUBMARKET_NAME", StringType(), False), \
            StructField("LATITUDE", DecimalType(19, 6), False),\
            StructField("LONGITUDE", DecimalType(19, 6), False)
        ]
        store_struc = StructType(fields=store_schema)
        store_dim = spark.read.csv(f's3a://hui-mid-term/{self.s3_store_csv_location}', header=True, schema=store_struc)

        product_schema= [
            StructField("PROD_KEY", IntegerType(), False), \
            StructField("PROD_NAME", StringType(), False), \
            StructField("VOL", DecimalType(38, 2), False), \
            StructField("BRAND_NAME", StringType(), False), \
            StructField("WGT", DecimalType(38, 2), False), \
            StructField("STATUS_CODE", StringType(), False), \
            StructField("STATUS_CODE_NAME", StringType(), False), \
            StructField("CATEGORY_KEY", IntegerType(), False), \
            StructField("CATEGORY_NAME", StringType(), False), \
            StructField("SUBCATEGORY_KEY", IntegerType(), False), \
            StructField("SUBCATEGORY_NAME", StringType(), False)
        ]
        product_struc = StructType(fields=product_schema)
        product_dim = spark.read.csv(f's3a://hui-mid-term/{self.s3_product_csv_location}', header=True, schema=product_struc)       

        sales_schema= [
            StructField("TRANS_ID", DateType(), False), \
            StructField("PROD_KEY", IntegerType(), False), \
            StructField("STORE_KEY", IntegerType(), False), \
            StructField("TRANS_DT", DateType(), False), \
            StructField("TRANS_TIME", IntegerType(), False), \
            StructField("SALES_QTY", DecimalType(38, 2), False), \
            StructField("SALES_PRICE", DecimalType(38, 2), False), \
            StructField("SALES_AMT", DecimalType(38, 2), False), \
            StructField("DISCOUNT", DecimalType(38, 2), False), \
            StructField("SALES_COST", DecimalType(38, 2), False), \
            StructField("SALES_MGRN", DecimalType(38, 2), False), \
            StructField("SHIP_COST", DecimalType(38, 2), False)
        ]
        sales_struc = StructType(fields=sales_schema)
        sales_dim = spark.read.csv(f's3a://hui-mid-term/{self.s3_sales_csv_location}', header=True, schema=sales_struc)

        inventory_schema= [
            StructField("CAL_DT", DateType(), False), \
            StructField("STORE_KEY", IntegerType(), False), \
            StructField("PROD_KEY", IntegerType(), False), \
            StructField("INVENTORY_ON_HAND_QTY", DecimalType(38, 2), False), \
            StructField("INVENTORY_ON_ORDER_QTY", DecimalType(38, 2), False), \
            StructField("OUT_OF_STOCK_FLG", IntegerType(), False), \
            StructField("WASTE_QTY", DecimalType(38, 2), False), \
            StructField("PROMOTION_FLG", BooleanType(), False), \
            StructField("NEXT_DELIVERY_DT", DateType(), False)
        ]
        inventory_struc = StructType(fields=inventory_schema)
        inventory_dim = spark.read.csv(f's3a://hui-mid-term/{self.s3_inventory_csv_location}', header=True, schema=inventory_struc)
        inventory_dim = inventory_dim.withColumn("OUT_OF_STOCK_FLG", when(inventory_dim.OUT_OF_STOCK_FLG == 1, True).otherwise(False))
        inventory_dim = inventory_dim.withColumn("OUT_OF_STOCK_FLG", inventory_dim.OUT_OF_STOCK_FLG.cast(BooleanType()))

        daily_sales_stg = sales_dim.groupBy("TRANS_DT", "STORE_KEY", "PROD_KEY").agg(SUM("SALES_QTY").alias("sales_qty"),\
                                                                                    SUM("SALES_AMT").alias("sales_amt"),\
                                                                                    avg("SALES_PRICE").alias("sales_price"),\
                                                                                    SUM("SALES_COST").alias("sales_cost"),\
                                                                                    SUM("SALES_MGRN").alias("sales_mgrn"),\
                                                                                    avg("DISCOUNT").alias("discount"),\
                                                                                    SUM("SHIP_COST").alias("ship_cost")
                                                                                    )\
                                                                                .withColumnRenamed("TRANS_DT", "cal_dt")\
                                                                                .withColumnRenamed("STORE_KEY", "store_key")\
                                                                                .withColumnRenamed("PROD_KEY", "prod_key")\
                                                                                .orderBy("cal_dt", "store_key", "prod_key")
        
        daily_inventory_stg = inventory_dim.withColumnRenamed("CAL_DT", "cal_dt")\
                .withColumnRenamed("STORE_KEY", "store_key")\
                .withColumnRenamed("PROD_KEY", "prod_key")\
                .withColumnRenamed("INVENTORY_ON_HAND_QTY", "stock_on_hand_qty")\
                .withColumnRenamed("INVENTORY_ON_ORDER_QTY", "ordered_stock")\
                .withColumnRenamed("OUT_OF_STOCK_FLG", "out_of_stock_flg")\
                .withColumnRenamed("WASTE_QTY", "waste_qty")\
                .withColumnRenamed("PROMOTION_FLG", "promotion_flg")\
                .withColumnRenamed("NEXT_DELIVERY_DT", "next_delivery_dt")

        sales_inv_store_dy = daily_sales_stg.join(daily_inventory_stg, ['cal_dt','store_key','prod_key'], how='fullouter')\
            .select(coalesce(daily_sales_stg.cal_dt, daily_inventory_stg.cal_dt).alias('cal_dt'),\
                    coalesce(daily_sales_stg.store_key, daily_inventory_stg.store_key).alias('store_key'),\
                    coalesce(daily_sales_stg.prod_key, daily_inventory_stg.prod_key).alias('prod_key'),\
                    coalesce(daily_sales_stg.sales_qty, lit(0)).alias('sales_qty'),\
                    coalesce(daily_sales_stg.sales_price, lit(0)).alias('sales_price'),\
                    coalesce(daily_sales_stg.sales_amt, lit(0)).alias('sales_amt'),\
                    coalesce(daily_sales_stg.discount, lit(0)).alias('discount'),\
                    coalesce(daily_sales_stg.sales_cost, lit(0)).alias('sales_cost'),\
                    coalesce(daily_sales_stg.sales_mgrn, lit(0)).alias('sales_mgrn'),\
                    coalesce(daily_inventory_stg.stock_on_hand_qty, lit(0)).alias('stock_on_hand_qty'),\
                    coalesce(daily_inventory_stg.ordered_stock, lit(0)).alias('ordered_stock_qty'),\
                    coalesce(daily_inventory_stg.out_of_stock_flg, lit(False)).alias('out_of_stock_flg'))
        sales_inv_store_dy = sales_inv_store_dy.withColumn("in_stock_flg", when(sales_inv_store_dy.out_of_stock_flg == True, False).otherwise(True))
        sales_inv_store_dy = sales_inv_store_dy.withColumn("low_stock_flg", when(sales_inv_store_dy.stock_on_hand_qty < sales_inv_store_dy.sales_qty, True).otherwise(False))

        sales_inv_store_wk = sales_inv_store_dy.join(calendar_dim, ['cal_dt'], how='inner')
        sales_inv_store_wk = sales_inv_store_wk.groupBy("YR_NUM", "WK_NUM", "store_key", "prod_key")\
            .agg(\
                    # Q1
                    SUM("sales_qty").alias("wk_sales_qty"),\
                    # Q2
                    SUM("sales_amt").alias("wk_sales_amt"),\
                    # Q3
                    avg("sales_price").alias("avg_sales_price"),\
                    # Q4
                    SUM(when(calendar_dim["DAY_OF_WK_NUM"] == 6, sales_inv_store_dy["stock_on_hand_qty"]).otherwise(0)).alias("eop_stock_on_hand_qty"),\
                    # Q5
                    SUM(when(calendar_dim["DAY_OF_WK_NUM"] == 6, sales_inv_store_dy["ordered_stock_qty"]).otherwise(0)).alias("eop_ordered_stock_qty"),\
                    # Q6
                    SUM("sales_cost").alias("wk_sales_cost"),\
                    # Q9
                    SUM(when(sales_inv_store_dy["low_stock_flg"] == True, sales_inv_store_dy["sales_amt"] - sales_inv_store_dy["stock_on_hand_qty"]).otherwise(0)).alias("potential_low_stock_impact"),\
                    # Q10
                    SUM(when(sales_inv_store_dy["out_of_stock_flg"] == True, sales_inv_store_dy["sales_amt"]).otherwise(0)).alias("no_stock_impact"),\
                    # Q11
                    count(when(sales_inv_store_dy["low_stock_flg"] == True, 1)).alias("low_stock_times"),\
                    # Q12
                    count(when(sales_inv_store_dy["out_of_stock_flg"] == True, 1)).alias("out_of_stock_times"),\
                    SUM("discount").alias("wk_discount"),\
                    SUM("sales_mgrn").alias("wk_sales_mgrn"),\
                    count(when(sales_inv_store_dy["in_stock_flg"] == True, 1)).alias("in_stock_times")\
                )
        # Q7
        sales_inv_store_wk = sales_inv_store_wk.withColumn("in_stock_percentage", 1 - sales_inv_store_wk.out_of_stock_times/7)
        # Q8
        sales_inv_store_wk = sales_inv_store_wk.withColumn("low_stock_impact", sales_inv_store_wk["out_of_stock_times"] + sales_inv_store_wk["low_stock_times"])
        # Q13
        sales_inv_store_wk = sales_inv_store_wk.withColumn("weeks_to_supply", sales_inv_store_wk["eop_stock_on_hand_qty"]/sales_inv_store_wk["wk_sales_qty"])

        sales_inv_store_wk = sales_inv_store_wk.withColumnRenamed("YR_NUM","yr_num")\
                .withColumnRenamed("WK_NUM","wk_num")\
                .orderBy("yr_num", "wk_num", "store_key", "prod_key")

        return (sales_inv_store_wk, store_dim, product_dim, calendar_dim)

    def writeCSV(self, df, output_file_url):
        df.write.option("header", "True").mode("overwrite").csv(output_file_url)
    
    def writeParquet(self, df, output_file_url):
        df.write.mode("overwrite").parquet(output_file_url)

    def runJob(self):
        sales_inv_store_wk, store_dim, product_dim, calendar_dim = self.etl(self.spark)

        self.writeParquet(sales_inv_store_wk, f"{self.output_file_url}{self.dateStr}/fact")
        self.writeCSV(store_dim, f"{self.output_file_url}{self.dateStr}/store_dim/")
        self.writeCSV(product_dim, f"{self.output_file_url}{self.dateStr}/product_dim/")
        self.writeCSV(calendar_dim, f"{self.output_file_url}{self.dateStr}/calendar_dim/")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--spark_name', help="spark_name")
    parser.add_argument('--calenar_csv_location')
    parser.add_argument('--inventory_csv_location')
    parser.add_argument('--product_csv_location')
    parser.add_argument('--sales_csv_location')
    parser.add_argument('--store_csv_location')

    args = parser.parse_args()
    spark_name = args.spark_name
    s3_calendar_location = args.calenar_csv_location
    s3_inventory_csv_location = args.inventory_csv_location
    s3_product_csv_location = args.product_csv_location
    s3_sales_csv_location = args.sales_csv_location
    s3_store_csv_location = args.store_csv_location
    spark_runner = SparkRunner(spark_name, s3_calendar_location, s3_inventory_csv_location, s3_product_csv_location, s3_sales_csv_location, s3_store_csv_location)
    spark_runner.runJob()

