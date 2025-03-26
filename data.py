from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("makeup")\
        .getOrCreate()

    print("read dataset.csv ... ")
    path_makeup = "dataset.csv"
    df_makeup = spark.read.csv(path_makeup, header=True, inferSchema=True)
    df_makeup.createOrReplaceTempView("makeup")

    # Describir la tabla
    query = 'DESCRIBE makeup'
    spark.sql(query).show(20)

    # Seleccionar productos con "limited_edition" marcado como True, ordenados por precio
    query = """SELECT product_name, brand_name, price_usd FROM makeup WHERE limited_edition = TRUE ORDER BY price_usd"""
    df_limited_edition = spark.sql(query)
    df_limited_edition.show(20)

    # Seleccionar productos con un precio entre 20 y 50 USD, ordenados de mayor a menor precio
    query = 'SELECT product_name, brand_name, price_usd FROM makeup WHERE price_usd BETWEEN 20 AND 50 ORDER BY price_usd DESC'
    df_mid_range = spark.sql(query)
    df_mid_range.show(20)

    # Guardar los resultados en JSON
    results = df_mid_range.toJSON().collect()
    df_mid_range.write.mode("overwrite").json("results")

    with open('results/data.json', 'w') as file:
        json.dump(results, file)

    # Contar productos por categoría primaria
    query = 'SELECT primary_category, COUNT(*) as count FROM makeup GROUP BY primary_category ORDER BY count DESC'
    df_category_count = spark.sql(query)
    df_category_count.show()
    
    spark.stop()
