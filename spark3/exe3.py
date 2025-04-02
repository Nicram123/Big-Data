from pyspark.sql.functions import *
from pyspark.sql import Window

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WindowFunctionsExample").getOrCreate()

# Dane dla Transactions
transactions_data = [
( 1, '2011-01-01', 500),
( 1, '2011-01-15', 50),
( 1, '2011-01-22', 250),
( 1, '2011-01-24', 75),
( 1, '2011-01-26', 125),
( 1, '2011-01-28', 175),
( 2, '2011-01-01', 500),
( 2, '2011-01-15', 50),
( 2, '2011-01-22', 25),
( 2, '2011-01-23', 125),
( 2, '2011-01-26', 200),
( 2, '2011-01-29', 250),
( 3, '2011-01-01', 500),
( 3, '2011-01-15', 50 ),
( 3, '2011-01-22', 5000),
( 3, '2011-01-25', 550),
( 3, '2011-01-27', 95 ),
( 3, '2011-01-30', 2500)
]

transactions_df = spark.createDataFrame(transactions_data, ["AccountId", "TranDate", "TranAmt"])
transactions_df = transactions_df.withColumn("TranDate", to_date("TranDate", "yyyy-MM-dd"))
transactions_df.show()

# Dane dla Logical
logical_data = [
(1,'George', 800),
(2,'Sam', 950),
(3,'Diane', 1100),
(4,'Nicholas', 1250),
(5,'Samuel', 1250),
(6,'Patricia', 1300),
(7,'Brian', 1500),
(8,'Thomas', 1600),
(9,'Fran', 2450),
(10,'Debbie', 2850),
(11,'Mark', 2975),
(12,'James', 3000),
(13,'Cynthia', 3000),
(14,'Christopher', 5000)
]

logical_df = spark.createDataFrame(logical_data, ["RowID", "FName", "Salary"])
logical_df.show()

window = Window.partitionBy("AccountId").orderBy("TranDate")
transactions_df.withColumn("RunTotalAmt", sum("TranAmt").over(window)).orderBy(["AccountId","TranDate"]).show()

transactions_df.withColumn("RunAvg", avg("TranAmt").over(window))\
    .withColumn("RunTranQty", count("*").over(window))\
    .withColumn("RunMinAmt", min("TranAmt").over(window))\
    .withColumn("RunMaxAmt", max("TranAmt").over(window))\
    .withColumn("RunTotalAmt", sum("TranAmt").over(window))\
    .orderBy(["AccountId","TranDate"]).show()

sliding_window = Window.partitionBy("AccountId").orderBy("TranDate").rowsBetween(-2, 0)
sliding_window_2 = Window.partitionBy("AccountId").orderBy("TranDate")
transactions_df.withColumn("SlideAvg", avg("TranAmt").over(sliding_window))\
    .withColumn("SlideQty", count("*").over(sliding_window))\
    .withColumn("SlideMin", min("TranAmt").over(sliding_window))\
    .withColumn("SlideMax", max("TranAmt").over(sliding_window))\
    .withColumn("SlideTotal", sum("TranAmt").over(sliding_window))\
    .withColumn("RowNumber", row_number().over(sliding_window_2))\
    .orderBy(["AccountId","TranDate", "RowNumber"]).show()

window_rows_unbounded = Window.orderBy("Salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
window_range_unbounded = Window.orderBy("Salary").rangeBetween(Window.unboundedPreceding, Window.currentRow)
logical_df.withColumn("SumByRows", sum("Salary").over(window_rows_unbounded))\
    .withColumn("SumByRange", sum("Salary").over(window_range_unbounded))\
    .orderBy("RowID").show()
