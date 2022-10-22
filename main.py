from re import sub
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, to_date, split
FILE_NAME = 'train.csv'


def camel_case(s):
    """rewrite string in camelCase format"""
    s = sub(r"(_|-)+", " ", s).title().replace(" ", "")
    return ''.join([s[0].lower(), s[1:]])


def extract_data():
    """create spark session and create dataset from extracted data"""
    spark = SparkSession.builder.appName('main').master('local').getOrCreate()
    df = spark.read.option('header', 'true').csv(FILE_NAME, inferSchema=True)

    return spark, df


def transform_data(spark, raw_df):
    """split original dataframe to Sales & Customers dataframes and transform data"""

    # remove insignificant columns from a dataframe
    significant_columns = ["Order ID", "Order Date", "Ship Date", "Ship Mode", "City",
                           "Customer ID", "Customer Name", "Segment", "Country"]
    raw_df = raw_df.dropDuplicates(significant_columns)

    # Rename column names in camelCase format
    corrected_column_names = [camel_case(i) for i in raw_df.columns]
    raw_df = raw_df.toDF(*corrected_column_names)

    # transform date from string format into date format
    raw_df = raw_df.withColumn('orderDate', to_date(raw_df.orderDate, "dd/MM/yyyy"))\
        .withColumn('shipDate', to_date(raw_df.shipDate, "dd/MM/yyyy"))

    # Create Sales dataframe from raw dataframe
    sales_columns = ["orderId", "orderDate", "shipDate", "shipMode", "city"]
    df_sales = raw_df.select(*sales_columns)

    # Change date format
    df_sales = df_sales.withColumn('orderDate', date_format(df_sales.orderDate, "yyyy/MM/dd"))\
        .withColumn('shipDate', date_format(df_sales.shipDate, "yyyy/MM/dd"))

    # Create Customers dataframe from raw dataframe
    customers_columns = ["customerId", "customerName", "segment", "country", "orderDate"]
    df_customers = raw_df.select(*customers_columns)

    df_customers.createOrReplaceTempView("customers")

    query = """
      SELECT tbl1.customerId, tbl1.customerName, tbl1.segment, tbl1.country, quantityOfOrdersLast5Days, totalQuantityOfOrders
      FROM(
          SELECT customerId, customerName, segment, country, sum(five_days) as quantityOfOrdersLast5Days
          FROM(
              SELECT *,
              CASE WHEN orderDate >= CURRENT_DATE - 1555
              THEN 1
              ELSE 0
              END AS five_days
              FROM customers) AS x
              GROUP BY customerId, customerName, segment, country
          ) tbl1
      JOIN (
          SELECT customerId, customerName, segment, country, count(1) as totalQuantityOfOrders
          FROM(
              SELECT * FROM customers) AS x 
          GROUP BY customerId, customerName, segment, country
    ) tbl2
    on tbl1.customerId = tbl2.customerId;
      """
    df_customers = spark.sql(query)

    # split customerName column to customerFirstName and customerLastName columns
    df_customers = df_customers.withColumn("customerFirstName",  split(df_customers.customerName, ' ').getItem(0))\
        .withColumn("customerLastName", split(df_customers.customerName, ' ', 2).getItem(1))

    return df_sales, df_customers


def display_transformed_data(dataframes):
    for i in dataframes:
        i.show()


if __name__ == '__main__':
    spark_and_df = extract_data()
    transformed_data = transform_data(spark_and_df[0], spark_and_df[1])
    display_transformed_data(transformed_data)
