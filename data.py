from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("cars")\
        .getOrCreate()

    print("read dataset.csv ... ")
    path_cars="dataset.csv"
    df_cars = spark.read.csv(path_cars, header=True, inferSchema=True)
    df_cars.createOrReplaceTempView("cars")

    query = 'DESCRIBE cars'
    spark.sql(query).show(20)

    query = """SELECT mark, model, year FROM cars WHERE fuel == "diesel" ORDER BY year"""
    df_cars_diesel = spark.sql(query)
    df_cars_diesel.show(20)

    query = 'SELECT mark, model, price FROM cars WHERE year BETWEEN 2000 AND 2010 ORDER BY price DESC'
    df_cars_2000_2010 = spark.sql(query)
    df_cars_2000_2010.show(20)
    results = df_cars_2000_2010.toJSON().collect()
    
    df_cars_2000_2010.write.mode("overwrite").json("results")
    
    with open('results/data.json', 'w') as file:
        json.dump(results, file)

    query = 'SELECT fuel, COUNT(fuel) FROM cars WHERE year BETWEEN 1990 AND 2000 GROUP BY fuel'
    df_cars_90s_fuel = spark.sql(query)
    df_cars_90s_fuel.show()
    
    spark.stop()
