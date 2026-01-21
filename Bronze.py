
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType


# =================================
#     customer table
# =================================
address_schema = StructType([
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True)
])


@dlt.table(
    name="bronze_customers",
    comment="Bronze customers: safe parsing, schema preserved, DQ flags only"
)
def bronze_customers():

    df = spark.read.json("/Volumes/insurance_project/bronze_schema/raw/customers/")
    dup_window = Window.partitionBy("customer_id")

    return (
        df
        # ---- DOB: multiple formats, same column ----
        .withColumn(
            "dob",
            F.coalesce(
                F.to_date("dob", "yyyy-MM-dd"),
                F.to_date("dob", "dd-MM-yyyy"),
                F.to_date("dob", "MM/dd/yyyy"),
                F.to_date("dob", "yyyy/MM/dd"),
                F.to_date("dob", "dd/MM/yyyy")
            )
        )

        # ---- updated_at: timestamp formats ----
        .withColumn(
            "updated_at",
            F.coalesce(
                        F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss"),
                        F.to_timestamp("updated_at", "yyyy-MM-dd'T'HH:mm:ss"),
                        F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss.SSS"),
                        F.to_timestamp("updated_at", "yyyy-MM-dd'T'HH:mm:ss.SSS"),
                        F.to_timestamp("updated_at", "dd-MM-yyyy HH:mm:ss"),
                        F.to_timestamp("updated_at", "MM/dd/yyyy HH:mm:ss"),
                        F.to_timestamp("updated_at", "yyyy/MM/dd HH:mm:ss"),
                        F.to_timestamp("updated_at", "dd/MM/yyyy HH:mm:ss"),
                        F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm"),
                        F.to_timestamp("updated_at", "dd-MM-yyyy HH:mm"),
                        F.to_timestamp("updated_at", "MM/dd/yyyy HH:mm"),
                        F.to_timestamp("updated_at", "yyyy/MM/dd HH:mm"),
                        F.to_timestamp("updated_at", "dd/MM/yyyy HH:mm"),
                        F.to_timestamp("updated_at")
            )
        )

        # ---- credit_score: numeric safety ----
        .withColumn("credit_score", F.col("credit_score").cast("int"))

        # ---- address: try parse, keep original ----
        .withColumn("address_struct", F.from_json("address", address_schema))

        # ---- DQ flags ----
        .withColumn("is_duplicate_customer", F.count("*").over(dup_window) > 1)
        .withColumn(
            "invalid_credit_score",
            (F.col("credit_score") < 300) | (F.col("credit_score") > 850)
        )
        .withColumn(
            "invalid_dob",
            F.col("dob").isNull() & F.col("updated_at").isNotNull()
        )
        .withColumn(
            "invalid_address_json",
            F.col("address_struct").isNull() & F.col("address").isNotNull()
        )
    )


# =================================
#     POLICIES
# =================================

@dlt.table(
    name="bronze_policies",
    comment="Bronze policies: safe date parsing, overlap detection, schema preserved"
)
def bronze_policies():

    df = spark.read.json("/Volumes/insurance_project/bronze_schema/raw/policies/")
    dup_window = Window.partitionBy("policy_id")
    overlap_window = Window.partitionBy("customer_id", "vehicle_id")

    return (
        df
        # ---- Dates ----
        .withColumn(
            "start_date",
            F.coalesce(
                F.to_date("start_date", "yyyy-MM-dd"),
                F.to_date("start_date", "dd-MM-yyyy"),
                F.to_date("start_date", "MM/dd/yyyy"),
                F.to_date("start_date", "yyyy/MM/dd"),
                F.to_date("start_date", "dd/MM/yyyy")
            )
        )
        .withColumn(
            "end_date",
            F.coalesce(
                F.to_date("end_date", "yyyy-MM-dd"),
                F.to_date("end_date", "dd-MM-yyyy"),
                F.to_date("end_date", "MM/dd/yyyy"),
                F.to_date("end_date", "yyyy/MM/dd"),
                F.to_date("end_date", "dd/MM/yyyy")
            )
        )

        # ---- Standardization ----
        .withColumn("policy_type", F.upper("policy_type"))
        .withColumn("premium_amount", F.col("premium_amount").cast("double"))
        .withColumn("coverage_limit", F.col("coverage_limit").cast("double"))

        # ---- DQ flags ----
        .withColumn("is_duplicate_policy", F.count("*").over(dup_window) > 1)
        .withColumn("invalid_premium_amount", F.col("premium_amount") < 0)
        .withColumn("invalid_coverage_limit", F.col("coverage_limit") < 0)
        .withColumn(
            "invalid_policy_dates",
            F.col("start_date").isNull() |
            (F.col("end_date").isNotNull() & (F.col("start_date") > F.col("end_date")))
        )
        .withColumn(
            "has_overlapping_policies",
            F.count("*").over(overlap_window) > 1
        )
    )

# =================================
#     CLAIMS
# =================================

@dlt.table(
    name="bronze_claims",
    comment="Bronze claims: robust date parsing, casing standardization"
)
def bronze_claims():

    df = spark.read.json("/Volumes/insurance_project/bronze_schema/raw/claims/")
    dup_window = Window.partitionBy("claim_id")

    return (
        df
        # ---- Dates ----
        .withColumn(
            "incident_date",
            F.coalesce(
                F.to_date("incident_date", "yyyy-MM-dd"),
                F.to_date("incident_date", "dd-MM-yyyy"),
                F.to_date("incident_date", "MM-DD-yyyy"),
                F.to_date("incident_date", "MM/dd/yyyy"),
                F.to_date("incident_date", "yyyy/MM/dd"),
                F.to_date("incident_date", "dd/MM/yyyy")            )
        )
        .withColumn(
            "filed_date",
            F.coalesce(
                F.to_date("filed_date", "yyyy-MM-dd"),
                F.to_date("filed_date", "dd-MM-yyyy"),
                F.to_date("filed_date", "MM/dd/yyyy"),
                F.to_date("filed_date", "yyyy/MM/dd"),
                F.to_date("filed_date", "dd/MM/yyyy")
            )
        )

        # ---- Standardization ----
        .withColumn("claim_amount", F.col("claim_amount").cast("double"))
        .withColumn("status", F.upper("status"))
        .withColumn("claim_type", F.upper("claim_type"))

        # ---- DQ flags ----
        .withColumn("is_duplicate_claim", F.count("*").over(dup_window) > 1)
        .withColumn("invalid_claim_amount", F.col("claim_amount") < 0)
        .withColumn(
            "invalid_incident_date",
            F.col("incident_date").isNull() & F.col("filed_date").isNotNull()
        )
    )

# =================================
#     PAYOUTS
# =================================

@dlt.table(
    name="bronze_payouts",
    comment="Bronze payouts: safe date parsing, schema preserved"
)
def bronze_payouts():

    df = spark.read.json("/Volumes/insurance_project/bronze_schema/raw/payouts/")

    return (
        df
        .withColumn(
            "payment_date",
            F.coalesce(
                F.to_date("payment_date", "yyyy-MM-dd"),
                F.to_date("payment_date", "dd-MM-yyyy"),
                F.to_date("payment_date", "MM/dd/yyyy"),
                F.to_date("payment_date", "yyyy/MM/dd"),
                F.to_date("payment_date", "dd/MM/yyyy")
            )
        )
        .withColumn("amount_paid", F.col("amount_paid").cast("double"))
        .withColumn("payment_method", F.upper("payment_method"))

        .withColumn("invalid_amount_paid", F.col("amount_paid") < 0)
        .withColumn("invalid_payment_date", F.col("payment_date").isNull())
    )

# =================================
#    IOT TELEMATICS
# =================================

@dlt.table(
    name="bronze_iot_telematics",
    comment="Bronze telematics: safe timestamp parsing, no aggregation"
)
def bronze_iot_telematics():

    df = spark.read.json("/Volumes/insurance_project/bronze_schema/raw/iot_telematics/")

    return (
        df
        .withColumn(
            "timestamp",
            F.coalesce(
                F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp("timestamp", "dd-MM-yyyy HH:mm:ss"),
                F.to_timestamp("timestamp", "MM/dd/yyyy HH:mm:ss"),
                F.to_timestamp("timestamp", "yyyy/MM/dd HH:mm:ss"),
                F.to_timestamp("timestamp", "dd/MM/yyyy HH:mm:ss"),
                F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss"),
                F.to_timestamp("timestamp")
            )
        )
        .withColumn("speed", F.col("speed").cast("double"))
        .withColumn("mileage", F.col("mileage").cast("double"))
        .withColumn("hard_braking_event", F.col("hard_braking_event").cast("boolean"))

        .withColumn("invalid_speed", F.col("speed") < 0)
        .withColumn("invalid_mileage", F.col("mileage") < 0)
        .withColumn("invalid_timestamp", F.col("timestamp").isNull())
    )