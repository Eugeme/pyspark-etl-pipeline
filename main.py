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
    return df


def transform_data(raw_df):
    """split original dataframe to Sales & Customers dataframes and transform data"""

    # remove insignificant columns from a dataframe
    significant_columns = ["Order ID", "Order Date", "Ship Date", "Ship Mode", "City",
                           "Customer ID", "Customer Name", "Segment", "Country"]
    raw_df = raw_df.dropDuplicates(significant_columns)

    # Rename column names in camelCase format
    corrected_column_names = [camel_case(i) for i in raw_df.columns]
    raw_df = raw_df.toDF(*corrected_column_names)

    # Create Sales dataframe from raw dataframe
    sales_columns = ["orderId", "orderDate", "shipDate", "shipMode", "city"]
    df_sales = raw_df.select(*sales_columns)

    # Change date format
    df_sales = df_sales.withColumn('orderDate', date_format(to_date(df_sales.orderDate, "dd/MM/yyyy"), "yyyy/MM/dd"))\
        .withColumn('shipDate', date_format(to_date(df_sales.shipDate, "dd/MM/yyyy"), "yyyy/MM/dd"))

    # Create Customers dataframe from raw dataframe
    customers_columns = ["customerId", "customerName", "segment", "country"]
    df_customers = raw_df.select(*customers_columns)

    # add total quantity of orders per customer
    df_customers = df_customers.groupBy("customerId", "customerName", "segment", "country").count()\
        .withColumnRenamed("count", "totalQuantityOfOrders")

    # split customerName column to customerFirstName and customerLastName columns
    df_customers = df_customers.withColumn("customerFirstName",  split(df_customers.customerName, ' ').getItem(0))\
        .withColumn("customerLastName", split(df_customers.customerName, ' ', 2).getItem(1))

    return df_sales, df_customers


def display_transformed_data(dataframes):
    for i in dataframes:
        i.show()


if __name__ == '__main__':
    extracted_df = extract_data()
    transformed_data = transform_data(extracted_df)
    display_transformed_data(transformed_data)
