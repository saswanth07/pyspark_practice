from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("StudentAnalysis").getOrCreate()

# Load data from CSV
df = spark.read.option("header", "true").csv("input/students.csv")

# Show top 5 records
print("🔍 Showing top 5 rows:")
df.show(5)

# Convert score column to int and find average
df = df.withColumn("score", df["score"].cast("int"))
avg_score = df.groupBy("class").avg("score")

# Show average score per class
print("📊 Average Score per Class:")
avg_score.show()

spark.stop()
