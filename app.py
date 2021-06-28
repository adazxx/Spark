from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .appName("reading csv") \
        .config("spark.driver.extraClassPath", "/usr/local/spark/jars/mysql-connector-java-8.0.25.jar") \
        .getOrCreate()
    data_file = '/Users/adazhong/sparkproj/supermarket_sales.csv'
    sdfData = scSpark.read.csv(data_file, header=True, sep=",").cache()

    # group by gender
    gender = sdfData.groupBy("Gender").count()
    print(gender.show())

    # create a temp table
    sdfData.createOrReplaceTempView("sales")
    output = scSpark.sql('SELECT * from sales')
    output.show()

    # query for unit price < 15 and quantity < 10
    output = scSpark.sql('SELECT * from sales WHERE `Unit Price` < 15 AND Quantity < 10')
    output.show()

    # count sales by city
    output = scSpark.sql('SELECT COUNT(*) as total, City from sales GROUP BY City')
    output.show()
    output.write.format('json').save('filtered.json') # output to json

    # load output data into MySQL
    output.write.format('jdbc').options(
        url='jdbc:mysql://localhost:3306/spark',
        driver='com.mysql.cj.jdbc.Driver',
        dbtable='city_info',
        user='root',
        password='Root_123').mode('append').save()