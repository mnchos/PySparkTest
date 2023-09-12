import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ProductCategoryPairs").getOrCreate()
#создаем датафреймы для продуктов и категорий
products_data = [(1,"Стол",), (2,"Стул",), (3,"Ложка",), (4,"Вилка",), (5,"Самолет",),(6,"Дом")]
products_schema = ["Product_id","Product"]
products_df = spark.createDataFrame(products_data, products_schema)
categories_data = [(1,"Мебель",), (2,"Прибор",), (3,"Техника",)]
categories_schema = ["Category_id","Category"]
categories_df = spark.createDataFrame(categories_data, categories_schema)
#создаем датафрейм для связи продуктов с категориями
prodcat=spark.createDataFrame([
    {"id":1,"prodid":1,"catid":1},
    {"id":2,"prodid":2,"catid":1},
    {"id":3,"prodid":3,"catid":2},
    {"id":4,"prodid":4,"catid":2},
    {"id":5,"prodid":5,"catid":3},
    {"id":6,"prodid":1,"catid":2}
])
#создаем и выводим датафрейм который показывает пары «Имя продукта – Имя категории» и продукты без категорий
result_df = products_df.join(prodcat, products_df.Product_id == prodcat.prodid, "left") \
                      .join(categories_df, prodcat.catid == categories_df.Category_id, "left") \
                      .select(products_df.Product.alias("Product"), categories_df.Category.alias("Category"))
# Выводим результат 
result_df.show()