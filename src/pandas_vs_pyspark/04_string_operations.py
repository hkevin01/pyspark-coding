"""
Pandas vs PySpark: String Operations Comparison
===============================================
This script demonstrates string manipulation in both frameworks.
"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Sample data with strings
data = {
    "id": [1, 2, 3, 4, 5],
    "name": ["  Alice Smith  ", "BOB JONES", "charlie brown", "David Lee", "eve wilson"],
    "email": ["alice@EXAMPLE.com", "bob@test.COM", "charlie@demo.org", "david@email.net", "eve@sample.IO"],
    "phone": ["(555) 123-4567", "555-987-6543", "(555)555-5555", "555.111.2222", "5551234567"],
    "description": ["Hello World", "Data Science", "Machine Learning", "Big Data Analytics", "Python Programming"]
}

print("=" * 80)
print("STRING OPERATIONS COMPARISON")
print("=" * 80)

# Create DataFrames
pandas_df = pd.DataFrame(data)
spark = SparkSession.builder.appName("StringOps").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
pyspark_df = spark.createDataFrame(pandas_df)

print("\nOriginal Data:")
print(pandas_df)

# ============================================================================
# 1. CHANGE CASE
# ============================================================================
print("\n" + "=" * 80)
print("1. CHANGE CASE")
print("=" * 80)

# Pandas
print("\nPANDAS - df['name'].str.upper():")
print(pandas_df["name"].str.upper())

print("\nPANDAS - df['name'].str.lower():")
print(pandas_df["name"].str.lower())

print("\nPANDAS - df['name'].str.title():")
print(pandas_df["name"].str.title())

# PySpark
print("\nPYSPARK - F.upper(df['name']):")
pyspark_df.select("id", F.upper("name").alias("name_upper")).show()

print("PYSPARK - F.lower(df['name']):")
pyspark_df.select("id", F.lower("name").alias("name_lower")).show()

print("PYSPARK - F.initcap(df['name']) [Title Case]:")
pyspark_df.select("id", F.initcap("name").alias("name_title")).show()

# ============================================================================
# 2. TRIM WHITESPACE
# ============================================================================
print("\n" + "=" * 80)
print("2. TRIM WHITESPACE")
print("=" * 80)

# Pandas
print("\nPANDAS - df['name'].str.strip():")
print(pandas_df["name"].str.strip())

# PySpark
print("\nPYSPARK - F.trim(df['name']):")
pyspark_df.select("id", "name", F.trim("name").alias("name_trimmed")).show(truncate=False)

# ============================================================================
# 3. STRING LENGTH
# ============================================================================
print("\n" + "=" * 80)
print("3. STRING LENGTH")
print("=" * 80)

# Pandas
print("\nPANDAS - df['name'].str.len():")
print(pandas_df["name"].str.len())

# PySpark
print("\nPYSPARK - F.length(df['name']):")
pyspark_df.select("name", F.length("name").alias("name_length")).show()

# ============================================================================
# 4. CONTAINS / LIKE
# ============================================================================
print("\n" + "=" * 80)
print("4. CONTAINS / LIKE (Find names containing 'i')")
print("=" * 80)

# Pandas
print("\nPANDAS - df[df['name'].str.contains('i', case=False)]:")
result = pandas_df[pandas_df["name"].str.contains("i", case=False)]
print(result[["id", "name"]])

# PySpark
print("\nPYSPARK - df.filter(F.col('name').contains('i')):")
pyspark_df.filter(F.col("name").contains("i")).select("id", "name").show()

# ============================================================================
# 5. STARTS WITH / ENDS WITH
# ============================================================================
print("\n" + "=" * 80)
print("5. STARTS WITH / ENDS WITH")
print("=" * 80)

# Pandas
print("\nPANDAS - Names starting with 'B' or 'b':")
result = pandas_df[pandas_df["name"].str.lower().str.startswith("b")]
print(result[["id", "name"]])

print("\nPANDAS - Emails ending with '.com':")
result = pandas_df[pandas_df["email"].str.endswith(".com", na=False)]
print(result[["id", "email"]])

# PySpark
print("\nPYSPARK - Names starting with 'B':")
pyspark_df.filter(F.lower(F.col("name")).startswith("b")).select("id", "name").show()

print("PYSPARK - Emails ending with '.com':")
pyspark_df.filter(F.col("email").endswith(".com")).select("id", "email").show()

# ============================================================================
# 6. STRING REPLACEMENT
# ============================================================================
print("\n" + "=" * 80)
print("6. STRING REPLACEMENT")
print("=" * 80)

# Pandas
print("\nPANDAS - Replace 'Smith' with 'Johnson':")
print(pandas_df["name"].str.replace("Smith", "Johnson"))

print("\nPANDAS - Remove all non-digits from phone:")
print(pandas_df["phone"].str.replace(r"[^0-9]", "", regex=True))

# PySpark
print("\nPYSPARK - Replace 'Smith' with 'Johnson':")
pyspark_df.select(
    "name", 
    F.regexp_replace("name", "Smith", "Johnson").alias("name_replaced")
).show(truncate=False)

print("PYSPARK - Remove all non-digits from phone:")
pyspark_df.select(
    "phone", 
    F.regexp_replace("phone", r"[^0-9]", "").alias("phone_digits")
).show()

# ============================================================================
# 7. STRING SPLITTING
# ============================================================================
print("\n" + "=" * 80)
print("7. STRING SPLITTING")
print("=" * 80)

# Pandas
print("\nPANDAS - Split name into first and last:")
name_parts = pandas_df["name"].str.strip().str.split(" ", n=1, expand=True)
name_parts.columns = ["first_name", "last_name"]
print(name_parts)

print("\nPANDAS - Split email to get domain:")
print(pandas_df["email"].str.split("@", expand=True)[1])

# PySpark
print("\nPYSPARK - Split name (get first part):")
pyspark_df.select(
    "name",
    F.split(F.trim("name"), " ")[0].alias("first_name")
).show()

print("PYSPARK - Split email to get domain:")
pyspark_df.select(
    "email",
    F.split("email", "@")[1].alias("domain")
).show()

# ============================================================================
# 8. CONCATENATION
# ============================================================================
print("\n" + "=" * 80)
print("8. STRING CONCATENATION")
print("=" * 80)

# Pandas
print("\nPANDAS - Combine id and name:")
print(pandas_df["id"].astype(str) + " - " + pandas_df["name"])

# PySpark
print("\nPYSPARK - Combine id and name:")
pyspark_df.select(
    F.concat(F.col("id").cast("string"), F.lit(" - "), F.col("name")).alias("id_name")
).show(truncate=False)

# ============================================================================
# 9. EXTRACT PATTERNS (REGEX)
# ============================================================================
print("\n" + "=" * 80)
print("9. EXTRACT PATTERNS (Extract domain from email)")
print("=" * 80)

# Pandas
print("\nPANDAS - Extract domain using regex:")
domains = pandas_df["email"].str.extract(r"@(\w+\.\w+)", expand=False)
print(domains)

# PySpark
print("\nPYSPARK - Extract domain using regexp_extract:")
pyspark_df.select(
    "email",
    F.regexp_extract("email", r"@(\w+\.\w+)", 1).alias("domain")
).show()

# ============================================================================
# 10. SUBSTRING
# ============================================================================
print("\n" + "=" * 80)
print("10. SUBSTRING (First 3 characters of name)")
print("=" * 80)

# Pandas
print("\nPANDAS - df['name'].str[:3]:")
print(pandas_df["name"].str[:3])

# PySpark
print("\nPYSPARK - F.substring('name', 1, 3):")
pyspark_df.select(
    "name",
    F.substring("name", 1, 3).alias("name_prefix")
).show()

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("KEY DIFFERENCES SUMMARY:")
print("=" * 80)
print("""
1. Case Conversion:
   - Pandas: df['col'].str.upper() / .lower() / .title()
   - PySpark: F.upper('col') / F.lower('col') / F.initcap('col')

2. Trimming:
   - Pandas: df['col'].str.strip() / .lstrip() / .rstrip()
   - PySpark: F.trim('col') / F.ltrim('col') / F.rtrim('col')

3. Length:
   - Pandas: df['col'].str.len()
   - PySpark: F.length('col')

4. Contains:
   - Pandas: df['col'].str.contains('pattern')
   - PySpark: F.col('col').contains('pattern')

5. Replace:
   - Pandas: df['col'].str.replace('old', 'new', regex=True)
   - PySpark: F.regexp_replace('col', 'pattern', 'replacement')

6. Split:
   - Pandas: df['col'].str.split('delimiter')
   - PySpark: F.split('col', 'delimiter')[index]

7. Concatenation:
   - Pandas: df['col1'] + ' ' + df['col2']
   - PySpark: F.concat(F.col('col1'), F.lit(' '), F.col('col2'))

8. Substring:
   - Pandas: df['col'].str[start:end]
   - PySpark: F.substring('col', start, length)

9. Pattern Extraction:
   - Pandas: df['col'].str.extract(r'pattern')
   - PySpark: F.regexp_extract('col', r'pattern', group)

10. Key Point: Pandas uses .str accessor, PySpark uses F functions!
""")

spark.stop()
print("\n" + "=" * 80)
print("Script completed!")
print("=" * 80)
