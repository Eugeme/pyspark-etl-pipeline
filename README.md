# Pre-requisite #
* raw data transformed from csv file to a pyspark dataframe
* main dataframe splitted to customers and sales dataframes
* for customers dataframe <total quantity of orders> and <quantity of orders in last 5 days> per customer are calculated and added as separate columns
* column nmames rewritten with camelCase format
* date format is changed
* final dataframes are written to parquet files
## raw data ##
<img width="1414" alt="Screenshot 2022-10-24 at 11 51 50" src="https://user-images.githubusercontent.com/66912450/197499955-f6d97de7-41be-41c2-ab07-c424a9fc3d93.png">

## customers dataframe ##
<img width="1415" alt="Screenshot 2022-10-24 at 11 45 04" src="https://user-images.githubusercontent.com/66912450/197499772-d3344d42-3cb4-4851-875e-39c54c3165d7.png">

## sales dataframe ##
<img width="1013" alt="Screenshot 2022-10-24 at 11 45 46" src="https://user-images.githubusercontent.com/66912450/197499916-4deed09e-c603-4d70-a1ea-1380141ed3c1.png">


