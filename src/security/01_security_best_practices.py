#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
PYSPARK SECURITY BEST PRACTICES - Complete Guide
================================================================================

MODULE OVERVIEW:
----------------
This module provides comprehensive security best practices for Apache Spark
applications. Security is CRITICAL in production environments where you're
processing sensitive data, connecting to secure clusters, and deploying
applications that handle authentication and authorization.

Common security vulnerabilities in Spark applications include:
• Hardcoded credentials and API keys
• Unencrypted data transmission
• Missing authentication/authorization
• Insecure configuration settings
• Vulnerable dependencies
• Exposed Spark UI with sensitive data
• Insufficient access controls
• Logging sensitive information

PURPOSE:
--------
Learn how to:
1. Secure credentials and secrets management
2. Enable encryption (at-rest and in-transit)
3. Implement authentication and authorization
4. Configure secure Spark clusters
5. Protect sensitive data in logs and UI
6. Handle compliance requirements (GDPR, HIPAA, etc.)
7. Audit and monitor security events
8. Follow secure coding practices

TARGET AUDIENCE:
----------------
• Data engineers deploying production Spark applications
• Security engineers implementing data security policies
• DevOps teams managing Spark clusters
• Compliance officers ensuring regulatory compliance
• Anyone handling sensitive or PII data with Spark

================================================================================
SECURITY THREAT MODEL:
================================================================================

COMMON ATTACK VECTORS IN SPARK:

┌─────────────────────────────────────────────────────────────────┐
│                    SPARK SECURITY PERIMETER                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. NETWORK LAYER                                               │
│     ┌────────────────────────────────────────┐                 │
│     │ Client → Driver → Executors            │                 │
│     │ ⚠️  Unencrypted traffic vulnerable      │                 │
│     │ ✅ Solution: Enable SSL/TLS             │                 │
│     └────────────────────────────────────────┘                 │
│                                                                  │
│  2. AUTHENTICATION                                              │
│     ┌────────────────────────────────────────┐                 │
│     │ Who can submit Spark jobs?             │                 │
│     │ ⚠️  No auth = anyone can run code       │                 │
│     │ ✅ Solution: Kerberos, OAuth, LDAP      │                 │
│     └────────────────────────────────────────┘                 │
│                                                                  │
│  3. AUTHORIZATION                                               │
│     ┌────────────────────────────────────────┐                 │
│     │ What data can users access?            │                 │
│     │ ⚠️  No ACLs = access to all data        │                 │
│     │ ✅ Solution: Ranger, ACLs, Row-level    │                 │
│     └────────────────────────────────────────┘                 │
│                                                                  │
│  4. DATA ENCRYPTION                                             │
│     ┌────────────────────────────────────────┐                 │
│     │ In-Transit: Network encryption         │                 │
│     │ At-Rest: Storage encryption            │                 │
│     │ ⚠️  Plain text = data exposure          │                 │
│     │ ✅ Solution: AES-256, TLS 1.2+          │                 │
│     └────────────────────────────────────────┘                 │
│                                                                  │
│  5. SECRETS MANAGEMENT                                          │
│     ┌────────────────────────────────────────┐                 │
│     │ Credentials, API keys, passwords       │                 │
│     │ ⚠️  Hardcoded = Git history exposure    │                 │
│     │ ✅ Solution: Vault, KMS, Secrets Mgr    │                 │
│     └────────────────────────────────────────┘                 │
│                                                                  │
│  6. SPARK UI EXPOSURE                                           │
│     ┌────────────────────────────────────────┐                 │
│     │ Shows queries, data samples, configs   │                 │
│     │ ⚠️  Public UI = sensitive data leak     │                 │
│     │ ✅ Solution: Authentication, filtering  │                 │
│     └────────────────────────────────────────┘                 │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

SECURITY LAYERS (Defense in Depth):
Layer 1: Network Security (firewalls, VPCs, security groups)
Layer 2: Authentication (prove who you are)
Layer 3: Authorization (prove what you can do)
Layer 4: Encryption (protect data confidentiality)
Layer 5: Auditing (track what happened)
Layer 6: Monitoring (detect anomalies)

================================================================================
BEST PRACTICE #1: SECURE SECRETS MANAGEMENT
================================================================================

❌ WRONG - Hardcoded Credentials:
───────────────────────────────────
# NEVER DO THIS!
spark = SparkSession.builder \\
    .config("spark.hadoop.fs.s3a.access.key", "AKIAIOSFODNN7EXAMPLE") \\
    .config("spark.hadoop.fs.s3a.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY") \\
    .getOrCreate()

# Database password in code
jdbc_url = "jdbc:postgresql://db.example.com:5432/mydb?user=admin&password=Secret123!"

WHY THIS IS DANGEROUS:
• Credentials committed to Git (visible in history forever!)
• Visible in Spark UI and logs
• Shared across all users of the code
• No credential rotation
• Security audit failures

✅ CORRECT - Use Secrets Managers:
──────────────────────────────────

OPTION 1: Environment Variables (basic)
────────────────────────────────────────
import os

# Load from environment
aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

spark = SparkSession.builder \\
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \\
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \\
    .getOrCreate()

# Set environment variables in job submission:
# export AWS_ACCESS_KEY_ID=xxx
# export AWS_SECRET_ACCESS_KEY=yyy
# spark-submit app.py

OPTION 2: AWS Secrets Manager (production)
───────────────────────────────────────────
import boto3
import json

def get_secret(secret_name, region_name="us-east-1"):
    \"\"\"Retrieve secret from AWS Secrets Manager.\"\"\"
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

# Retrieve database credentials
db_creds = get_secret("prod/database/postgres")
jdbc_url = f"jdbc:postgresql://db.example.com:5432/mydb"

df = spark.read \\
    .format("jdbc") \\
    .option("url", jdbc_url) \\
    .option("dbtable", "users") \\
    .option("user", db_creds["username"]) \\
    .option("password", db_creds["password"]) \\
    .load()

OPTION 3: HashiCorp Vault
──────────────────────────
import hvac

# Connect to Vault
client = hvac.Client(url='https://vault.example.com:8200')
client.auth.approle.login(
    role_id=os.environ['VAULT_ROLE_ID'],
    secret_id=os.environ['VAULT_SECRET_ID']
)

# Read secrets
secrets = client.secrets.kv.v2.read_secret_version(
    path='spark/prod/database'
)

db_password = secrets['data']['data']['password']

OPTION 4: Databricks Secrets
─────────────────────────────
# Store secret: databricks secrets create-scope --scope prod
# Add secret: databricks secrets put --scope prod --key db_password

from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)

# Access secret (never appears in logs/UI)
db_password = dbutils.secrets.get(scope="prod", key="db_password")

jdbc_url = f"jdbc:postgresql://db.example.com:5432/mydb"
df = spark.read \\
    .format("jdbc") \\
    .option("url", jdbc_url) \\
    .option("user", "admin") \\
    .option("password", db_password) \\
    .load()

BEST PRACTICES:
✅ Never commit secrets to Git
✅ Use secrets managers (Vault, AWS Secrets Manager, Azure Key Vault)
✅ Rotate credentials regularly
✅ Use IAM roles instead of access keys when possible
✅ Limit secret access with least privilege
✅ Audit secret access

================================================================================
BEST PRACTICE #2: ENABLE ENCRYPTION
================================================================================

ENCRYPTION AT REST:
───────────────────

# Enable HDFS encryption zones (cluster admin)
hdfs crypto -createZone -keyName myKey -path /encrypted/data

# Enable S3 server-side encryption
spark.conf.set("spark.hadoop.fs.s3a.server-side-encryption-algorithm", "AES256")

# Enable disk encryption for shuffle and cache
spark = SparkSession.builder \\
    .config("spark.io.encryption.enabled", "true") \\
    .config("spark.io.encryption.keySizeBits", "256") \\
    .config("spark.io.encryption.keygen.algorithm", "HmacSHA256") \\
    .getOrCreate()

ENCRYPTION IN TRANSIT:
──────────────────────

# Enable SSL/TLS for Spark internal communication
spark = SparkSession.builder \\
    .config("spark.ssl.enabled", "true") \\
    .config("spark.ssl.protocol", "TLSv1.2") \\
    .config("spark.ssl.keyStore", "/path/to/keystore.jks") \\
    .config("spark.ssl.keyStorePassword", keystore_password) \\
    .config("spark.ssl.trustStore", "/path/to/truststore.jks") \\
    .config("spark.ssl.trustStorePassword", truststore_password) \\
    .getOrCreate()

# Enable SSL for Spark UI
spark.conf.set("spark.ui.https.enabled", "true")
spark.conf.set("spark.ui.https.keyStore", "/path/to/keystore.jks")
spark.conf.set("spark.ui.https.keyStorePassword", keystore_password)

# JDBC over SSL
jdbc_url = "jdbc:postgresql://db.example.com:5432/mydb?ssl=true&sslmode=require"

ENCRYPTION CHECKLIST:
✅ Enable encryption for shuffle files
✅ Enable encryption for RDD cache
✅ Use SSL/TLS for all network communication
✅ Encrypt data at rest (HDFS, S3, databases)
✅ Use TLS 1.2 or higher (not SSL 3.0 or TLS 1.0)
✅ Regularly rotate encryption keys

================================================================================
BEST PRACTICE #3: AUTHENTICATION & AUTHORIZATION
================================================================================

KERBEROS AUTHENTICATION (Hadoop clusters):
──────────────────────────────────────────

# Kerberos configuration
spark = SparkSession.builder \\
    .config("spark.security.credentials.hbase.enabled", "true") \\
    .config("spark.security.credentials.hive.enabled", "true") \\
    .config("spark.yarn.principal", "user@REALM.COM") \\
    .config("spark.yarn.keytab", "/path/to/user.keytab") \\
    .getOrCreate()

# Submit with Kerberos
# kinit -kt /path/to/user.keytab user@REALM.COM
# spark-submit --principal user@REALM.COM --keytab /path/to/user.keytab app.py

SPARK UI AUTHENTICATION:
────────────────────────

# Enable Spark UI authentication
spark.conf.set("spark.ui.filters", "org.apache.spark.ui.AclsFilter")
spark.conf.set("spark.acls.enable", "true")
spark.conf.set("spark.admin.acls", "admin_user")
spark.conf.set("spark.ui.view.acls", "user1,user2")

# Enable HTTP authentication
spark.conf.set("spark.authenticate", "true")
spark.conf.set("spark.authenticate.secret", shared_secret)

AUTHORIZATION (Apache Ranger):
──────────────────────────────

# Ranger provides fine-grained access control
# - Table-level access
# - Column-level access
# - Row-level filtering
# - Data masking

# Example: Read only allowed columns
df = spark.read.table("sensitive_data")  
# Ranger automatically filters columns based on user permissions
# User sees: id, name (allowed)
# User doesn't see: ssn, credit_card (denied)

LEAST PRIVILEGE PRINCIPLE:
✅ Users only get minimum required permissions
✅ Service accounts with specific roles
✅ Regular permission audits
✅ Remove unused accounts

================================================================================
BEST PRACTICE #4: SECURE SPARK UI
================================================================================

PROBLEM: Spark UI exposes sensitive information
───────────────────────────────────────────────

The Spark UI (port 4040) shows:
• SQL queries (may contain sensitive predicates)
• Job configurations (may include credentials)
• Environment variables
• Data samples
• File paths
• Executor details

SOLUTION 1: Enable Authentication
──────────────────────────────────

spark = SparkSession.builder \\
    .config("spark.ui.filters", "org.apache.spark.ui.AclsFilter") \\
    .config("spark.acls.enable", "true") \\
    .config("spark.admin.acls", "admin_user") \\
    .config("spark.ui.view.acls", "user1,user2") \\
    .config("spark.modify.acls", "admin_user") \\
    .getOrCreate()

SOLUTION 2: Redact Sensitive Data
──────────────────────────────────

# Redact sensitive values in UI
spark.conf.set("spark.redaction.regex", "(?i)(password|secret|token|key)")

# Example: This appears as ********** in UI
spark.conf.set("spark.my.api.key", "AKIAIOSFODNN7EXAMPLE")

SOLUTION 3: Disable Data Preview
─────────────────────────────────

# Disable showing data samples in SQL tab
spark.conf.set("spark.sql.ui.explainMode", "simple")

SOLUTION 4: Use Reverse Proxy
──────────────────────────────

# Put Spark UI behind authenticated reverse proxy (nginx, Apache)
# proxy_pass http://spark-ui:4040
# require valid-user

SOLUTION 5: Restrict Network Access
────────────────────────────────────

# Bind to localhost only (not 0.0.0.0)
spark.conf.set("spark.ui.host", "localhost")

# Use firewall rules to restrict access
# iptables -A INPUT -p tcp --dport 4040 -s trusted_ip -j ACCEPT

================================================================================
BEST PRACTICE #5: SECURE LOGGING
================================================================================

❌ WRONG - Logging Sensitive Data:
───────────────────────────────────

# NEVER DO THIS!
logger.info(f"Connecting with password: {password}")
logger.debug(f"API key: {api_key}")
df.show()  # May display PII

✅ CORRECT - Secure Logging:
────────────────────────────

# Mask sensitive values
def mask_sensitive(text):
    \"\"\"Mask sensitive data in logs.\"\"\"
    import re
    # Mask credit cards
    text = re.sub(r'\\b\\d{4}[\\s-]?\\d{4}[\\s-]?\\d{4}[\\s-]?\\d{4}\\b', 
                  'XXXX-XXXX-XXXX-XXXX', text)
    # Mask SSN
    text = re.sub(r'\\b\\d{3}-\\d{2}-\\d{4}\\b', 'XXX-XX-XXXX', text)
    # Mask emails
    text = re.sub(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}', 
                  '***@***.***', text)
    return text

logger.info(mask_sensitive(f"Processing user data: {data}"))

# Set log level to avoid verbose output
spark.sparkContext.setLogLevel("WARN")

# Redact in Spark logs
spark.conf.set("spark.redaction.regex", 
               "(?i)(password|pwd|secret|token|key|credit|ssn)")

# Don't log full DataFrames with sensitive data
# df.show()  # BAD
logger.info(f"Processed {df.count()} records")  # GOOD

================================================================================
BEST PRACTICE #6: INPUT VALIDATION & SQL INJECTION PREVENTION
================================================================================

❌ WRONG - SQL Injection Vulnerable:
─────────────────────────────────────

# String interpolation = SQL injection risk!
user_input = request.get("user_id")  # From web request
query = f"SELECT * FROM users WHERE user_id = {user_input}"
df = spark.sql(query)

# Attacker sends: user_id = "1 OR 1=1"
# Resulting query: SELECT * FROM users WHERE user_id = 1 OR 1=1
# Result: Returns ALL users!

✅ CORRECT - Parameterized Queries:
───────────────────────────────────

# Use DataFrame API (safe by default)
user_input = request.get("user_id")
df = spark.table("users").filter(col("user_id") == user_input)

# Or use parameterized SQL (Spark 3.4+)
df = spark.sql(
    "SELECT * FROM users WHERE user_id = :user_id",
    args={"user_id": user_input}
)

# Validate and sanitize inputs
def validate_user_id(user_id):
    \"\"\"Validate user ID format.\"\"\"
    if not user_id.isdigit():
        raise ValueError("Invalid user_id format")
    return int(user_id)

safe_user_id = validate_user_id(user_input)
df = spark.table("users").filter(col("user_id") == safe_user_id)

INPUT VALIDATION CHECKLIST:
✅ Validate all external inputs
✅ Use DataFrame API instead of raw SQL strings
✅ Never concatenate user input into SQL
✅ Whitelist allowed values when possible
✅ Use type checking and bounds checking

================================================================================
BEST PRACTICE #7: SECURE DEPENDENCIES
================================================================================

VULNERABILITY SCANNING:
───────────────────────

# Scan Python dependencies for vulnerabilities
pip install safety
safety check

# Scan requirements.txt
safety check -r requirements.txt

# Example output:
# ╒══════════════════════════════════════════════════════╕
# │ VULNERABILITY FOUND: pyyaml < 5.4                    │
# │ Severity: HIGH                                       │
# │ CVE-2020-14343: Arbitrary code execution             │
# ╘══════════════════════════════════════════════════════╛

DEPENDENCY MANAGEMENT:
──────────────────────

# requirements.txt - Pin versions!
pyspark==3.5.0  # Not: pyspark>=3.0.0 (unpredictable)
pandas==2.1.0
numpy==1.24.0

# Use virtual environments
python -m venv spark_env
source spark_env/bin/activate
pip install -r requirements.txt

# Regularly update dependencies
pip list --outdated
pip install --upgrade pyspark

SECURE PACKAGE SOURCES:
───────────────────────

# Use trusted package repositories
pip install --index-url https://pypi.org/simple pyspark

# Verify package signatures (when available)
pip install --require-hashes -r requirements-hashes.txt

================================================================================
BEST PRACTICE #8: COMPLIANCE (GDPR, HIPAA, SOC 2)
================================================================================

GDPR REQUIREMENTS:
──────────────────

# 1. Data Minimization - Only collect necessary data
df_minimal = df.select("user_id", "order_date", "amount")  # Not all columns

# 2. Right to Erasure - Delete user data on request
user_id_to_delete = "12345"
df_anonymized = df.filter(col("user_id") != user_id_to_delete)

# 3. Data Anonymization
from pyspark.sql.functions import sha2, concat_ws

df_anonymized = df.withColumn(
    "user_id_hash",
    sha2(concat_ws("_", col("user_id"), lit("salt")), 256)
).drop("user_id")

# 4. Audit Trail - Log all data access
logger.info(f"User {current_user} accessed table users at {timestamp}")

HIPAA REQUIREMENTS:
───────────────────

# 1. Encryption (covered above)
# 2. Access Controls (covered above)
# 3. Audit Logging

def log_phi_access(user, table, action):
    \"\"\"Log PHI (Protected Health Information) access.\"\"\"
    audit_log = {
        "timestamp": datetime.now().isoformat(),
        "user": user,
        "table": table,
        "action": action,
        "ip_address": get_client_ip()
    }
    logger.info(f"PHI_ACCESS: {json.dumps(audit_log)}")

log_phi_access("doctor_smith", "patient_records", "READ")

# 4. De-identification
df_deidentified = df.drop("name", "ssn", "address", "phone")

================================================================================
COMMON SECURITY MISTAKES & FIXES:
================================================================================

MISTAKE #1: Exposed Cloud Storage Buckets
──────────────────────────────────────────
❌ Public S3 bucket: s3a://my-public-bucket/data
✅ Private bucket with IAM roles

MISTAKE #2: Overly Permissive IAM Policies
───────────────────────────────────────────
❌ Action: "s3:*", Resource: "*"
✅ Action: ["s3:GetObject"], Resource: "arn:aws:s3:::my-bucket/data/*"

MISTAKE #3: Weak Passwords
──────────────────────────
❌ password = "admin123"
✅ password = generate_secure_password(length=32)

MISTAKE #4: Missing TLS Certificate Validation
───────────────────────────────────────────────
❌ requests.get(url, verify=False)
✅ requests.get(url, verify=True)

MISTAKE #5: Logging Stack Traces with Secrets
──────────────────────────────────────────────
❌ logger.exception(f"Error: {e}")  # May expose secrets
✅ logger.error("Database connection failed")  # Generic message

MISTAKE #6: Unpatched Systems
─────────────────────────────
❌ Running Spark 2.4.0 (EOL, known vulnerabilities)
✅ Running Spark 3.5.0 (latest stable with security patches)

MISTAKE #7: Default Credentials
───────────────────────────────
❌ Username: admin, Password: admin
✅ Force password change on first login

================================================================================
SECURITY CHECKLIST:
================================================================================

BEFORE DEPLOYMENT:
☐ No hardcoded credentials in code
☐ Secrets stored in secrets manager
☐ Encryption enabled (at-rest and in-transit)
☐ Authentication configured
☐ Authorization rules implemented
☐ Spark UI authentication enabled
☐ Sensitive data redacted in logs/UI
☐ Input validation implemented
☐ Dependencies scanned for vulnerabilities
☐ Compliance requirements met (GDPR, HIPAA, etc.)
☐ Security testing completed
☐ Incident response plan documented

MONITORING:
☐ Log all authentication attempts
☐ Alert on failed authentications
☐ Monitor for unusual data access patterns
☐ Track configuration changes
☐ Review audit logs regularly

MAINTENANCE:
☐ Regular security patches
☐ Credential rotation schedule
☐ Access permission reviews
☐ Security training for team
☐ Vulnerability scanning (weekly/monthly)

================================================================================
RELATED RESOURCES:
================================================================================

Spark Security Documentation:
  https://spark.apache.org/docs/latest/security.html

OWASP Top 10:
  https://owasp.org/www-project-top-ten/

CIS Benchmarks:
  https://www.cisecurity.org/benchmark/apache_spark

AWS Security Best Practices:
  https://docs.aws.amazon.com/whitepapers/latest/aws-security-best-practices/

Databricks Security:
  https://docs.databricks.com/security/index.html

AUTHOR: PySpark Education Project
LICENSE: Educational Use - MIT License
VERSION: 1.0.0 - Security Best Practices Guide
CREATED: 2024
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha2, concat_ws
import os
import logging

# Configure secure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_secure_spark_session():
    """
    Create a SparkSession with security best practices enabled.
    
    This demonstrates proper security configuration.
    """
    print("=" * 80)
    print("CREATING SECURE SPARK SESSION")
    print("=" * 80)
    
    # Load credentials from environment (not hardcoded!)
    aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    
    spark = SparkSession.builder \
        .appName("SecureSparkApp") \
        .config("spark.io.encryption.enabled", "true") \
        .config("spark.io.encryption.keySizeBits", "256") \
        .config("spark.authenticate", "true") \
        .config("spark.redaction.regex", "(?i)(password|secret|token|key)") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .getOrCreate()
    
    # Set appropriate log level
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Secure Spark session created")
    print(f"   Encryption enabled: {spark.conf.get('spark.io.encryption.enabled')}")
    print(f"   Redaction enabled: {spark.conf.get('spark.redaction.regex')}")
    
    return spark


def demonstrate_data_anonymization():
    """Demonstrate GDPR-compliant data anonymization."""
    print("\n" + "=" * 80)
    print("DATA ANONYMIZATION (GDPR Compliance)")
    print("=" * 80)
    
    spark = create_secure_spark_session()
    
    # Sample sensitive data
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    
    data = [
        ("U001", "John Doe", "john@example.com", 30),
        ("U002", "Jane Smith", "jane@example.com", 25),
        ("U003", "Bob Johnson", "bob@example.com", 35)
    ]
    
    df = spark.createDataFrame(data, schema)
    
    print("\n🔒 Original Data (SENSITIVE):")
    df.show(truncate=False)
    
    # Anonymize with hash
    df_anonymized = df \
        .withColumn("user_id_hash", sha2(col("user_id"), 256)) \
        .withColumn("email_hash", sha2(col("email"), 256)) \
        .drop("user_id", "name", "email")
    
    print("\n✅ Anonymized Data (SAFE):")
    df_anonymized.show(truncate=False)
    
    spark.stop()


def main():
    """Run security demonstrations."""
    print("\n" + "🔒" * 40)
    print("PYSPARK SECURITY BEST PRACTICES")
    print("🔒" * 40)
    
    create_secure_spark_session()
    demonstrate_data_anonymization()
    
    print("\n" + "=" * 80)
    print("✅ SECURITY GUIDE COMPLETE")
    print("=" * 80)
    
    print("\n🔐 Key Takeaways:")
    print("   1. Never hardcode credentials")
    print("   2. Enable encryption everywhere")
    print("   3. Implement authentication & authorization")
    print("   4. Secure the Spark UI")
    print("   5. Redact sensitive data in logs")
    print("   6. Validate all inputs")
    print("   7. Keep dependencies updated")
    print("   8. Comply with regulations (GDPR, HIPAA)")


if __name__ == "__main__":
    main()
