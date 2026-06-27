"""
Module 3: Apache Spark — PySpark Solutions
==========================================
Dataset:
  - employee_data.csv   : emp_id, name, age, department, salary
  - department_data.csv : dept_id, department, location
  - transaction_data.csv: transaction_id, user_id, amount, status
"""

from pyspark.sql import SparkSession

# ── Initialise Spark session ──────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Module3_Spark") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")   # suppress verbose INFO logs

print("=" * 60)
print("Module 3: PySpark Solutions")
print("=" * 60)

# ─────────────────────────────────────────────────────────────────────────────
# Task 1 — Load employee_data.csv into a DataFrame
# ─────────────────────────────────────────────────────────────────────────────
print("\n--- Task 1: Load employee_data.csv ---")

employee_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("employee_data.csv")

employee_df.printSchema()
employee_df.show()

# ─────────────────────────────────────────────────────────────────────────────
# Task 2 — Filter rows where age > 40
# ─────────────────────────────────────────────────────────────────────────────
print("\n--- Task 2: Employees with age > 40 ---")

filtered_df = employee_df.filter(employee_df["age"] > 40)
filtered_df.show()

# ─────────────────────────────────────────────────────────────────────────────
# Task 3 — groupBy department and calculate sum of salary
# ─────────────────────────────────────────────────────────────────────────────
print("\n--- Task 3: Total Salary by Department ---")

salary_by_dept = employee_df \
    .groupBy("department") \
    .sum("salary") \
    .withColumnRenamed("sum(salary)", "total_salary")

salary_by_dept.show()

# ─────────────────────────────────────────────────────────────────────────────
# Task 4 — Join employee and department DataFrames on department
# ─────────────────────────────────────────────────────────────────────────────
print("\n--- Task 4: Join Employee and Department DataFrames ---")

department_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("department_data.csv")

joined_df = employee_df.join(department_df, on="department", how="inner")
joined_df.show()

# ─────────────────────────────────────────────────────────────────────────────
# Task 5 — Count distinct values in the department column
# ─────────────────────────────────────────────────────────────────────────────
print("\n--- Task 5: Count of Distinct Departments ---")

distinct_count = employee_df.select("department").distinct().count()
print(f"Number of distinct departments: {distinct_count}")

# ── Stop Spark session ────────────────────────────────────────────────────────
spark.stop()
print("\nSpark session stopped. All tasks complete.")
