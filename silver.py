# -------------------------------------------------
# SILVER
# -------------------------------------------------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dlt.table(
    name="dim_customers",
    comment="Silver customers dimension with SCD Type 2 history tracking and NULL handling"
)
def dim_customers():

    df = dlt.read("bronze_customers")

    # -------------------------------------------------
    # Standardization
    # -------------------------------------------------
    df = (
        df
        # name cleanup
        .withColumn("name_clean", F.lower(F.trim("name")))

        # occupation cleanup
        .withColumn("occupation_clean", F.initcap(F.trim("occupation")))

        # credit score: keep only valid range, else NULL
        .withColumn(
            "credit_score",
            F.when(F.col("credit_score").between(300, 850), F.col("credit_score"))
        )

        # flatten address
        .withColumn("street", F.col("address_struct.street"))
        .withColumn("city", F.col("address_struct.city"))
        .withColumn("state", F.col("address_struct.state"))
        .withColumn("zip_code", F.col("address_struct.zip"))

        # fallback for updated_at (important for SCD)
        .withColumn(
            "updated_at",
            F.coalesce(F.col("updated_at"), F.col("dob"))
        )
    )

    df = df.filter(F.col("name").isNotNull())

    df = (
        df
        .withColumn("occupation_clean",
                    F.coalesce(F.col("occupation_clean"), F.lit("UNKNOWN")))
        .withColumn("street",
                    F.coalesce(F.col("street"), F.lit("UNKNOWN")))
        .withColumn("city",
                    F.coalesce(F.col("city"), F.lit("UNKNOWN")))
        .withColumn("state",
                    F.coalesce(F.col("state"), F.lit("UNKNOWN")))
        .withColumn("zip_code",
                    F.coalesce(F.col("zip_code"), F.lit("UNKNOWN")))
    )


    dedup_window = Window.partitionBy(
        "name_clean", "dob"
    ).orderBy(F.col("updated_at").desc())

    df = (
        df
        .withColumn("rn", F.row_number().over(dedup_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    scd_window = Window.partitionBy("customer_id").orderBy("updated_at")

    df = (
        df
        .withColumn("valid_from", F.col("updated_at"))
        .withColumn("valid_to", F.lead("updated_at").over(scd_window))
        .withColumn("is_current", F.col("valid_to").isNull())
    )

    return df.select(
        "customer_id",
        "name",
        "dob",
        "street",
        "city",
        "state",
        "zip_code",
        "credit_score",
        "occupation_clean",
        "valid_from",
        "valid_to",
        "is_current"
    )

@dlt.table(
    name="fact_policy_risk_profile",
    comment="Silver fact table calculating policy-level risk with NULL & invalid value handling"
)
def fact_policy_risk_profile():

    policies = dlt.read("bronze_policies")
    telematics = dlt.read("bronze_iot_telematics")
    customers = dlt.read("dim_customers").filter("is_current = true")

    # -------------------------------------------------
    # Telematics aggregation
    # -------------------------------------------------
    tele_agg = (
        telematics
        .groupBy("policy_id")
        .agg(
            (F.max("mileage") - F.min("mileage")).alias("total_miles_driven"),
            F.sum(F.col("hard_braking_event").cast("int")).alias("hard_braking_count"),
            F.sum(F.when(F.col("speed") > 80, 1).otherwise(0))
                .alias("speeding_events_count")
        )
    )

    # -------------------------------------------------
    # Join
    # -------------------------------------------------
    df = (
        policies
        .join(customers, "customer_id", "inner")
        .join(tele_agg, "policy_id", "left")
    )

    # -------------------------------------------------
    # Handle invalid policy values (from Bronze flags)
    # -------------------------------------------------
    df = (
        df
        .withColumn(
            "premium_amount",
            F.when(F.col("premium_amount") > 0, F.col("premium_amount"))
        )
        .withColumn(
            "coverage_limit",
            F.when(F.col("coverage_limit") > 0, F.col("coverage_limit"))
        )
    )

    # -------------------------------------------------
    # Handle telematics NULLs
    # -------------------------------------------------
    df = df.fillna({
        "hard_braking_count": 0,
        "speeding_events_count": 0
    })

    # -------------------------------------------------
    # Risk score (safe)
    # -------------------------------------------------
    df = df.withColumn(
        "risk_score",
        F.when(
            F.col("total_miles_driven") > 0,
            (F.col("hard_braking_count") + F.col("speeding_events_count")) /
            (F.col("total_miles_driven") / 100)
        ).otherwise(0)
    )

    # -------------------------------------------------
    # Risk category (business default)
    # -------------------------------------------------
    df = df.withColumn(
        "risk_category",
        F.when(F.col("risk_score") <= 2, "LOW")
         .when(F.col("risk_score") <= 5, "MEDIUM")
         .otherwise("HIGH")
    )

    return df

@dlt.table(
    name="fact_claims_lifecycle",
    comment="Silver fact table tracking full claims lifecycle with business-safe NULL handling"
)
def fact_claims_lifecycle():

    claims = dlt.read("bronze_claims")
    policies = dlt.read("fact_policy_risk_profile")
    payouts = dlt.read("bronze_payouts")

    # -------------------------------------------------
    # Aggregate payouts per claim
    # -------------------------------------------------
    payout_agg = (
        payouts
        .groupBy("claim_id")
        .agg(
            F.sum("amount_paid").alias("total_amount_paid"),
            F.max("payment_date").alias("last_payment_date")
        )
    )

    # -------------------------------------------------
    # Join claims with policies and payouts
    # Orphan claims are intentionally dropped
    # -------------------------------------------------
    df = (
        claims
        .join(policies, "policy_id", "inner")
        .join(payout_agg, "claim_id", "left")
    )

    # -------------------------------------------------
    # Normalize payout amounts
    # -------------------------------------------------
    df = df.withColumn(
        "total_amount_paid",
        F.coalesce(F.col("total_amount_paid"), F.lit(0))
    )

    # -------------------------------------------------
    # Claim validity (handles active policies correctly)
    # -------------------------------------------------
    df = df.withColumn(
        "is_valid_claim",
        F.when(
            (F.col("incident_date") >= F.col("start_date")) &
            (
                F.col("end_date").isNull() |
                (F.col("incident_date") <= F.col("end_date"))
            ),
            F.lit(True)
        ).otherwise(F.lit(False))
    )

    # -------------------------------------------------
    # Reopened claims logic
    # CLOSED + late payout => REOPENED
    # -------------------------------------------------
    df = df.withColumn(
        "status",
        F.when(
            (F.col("status") == "CLOSED") &
            (F.col("last_payment_date") > F.col("filed_date")),
            "REOPENED"
        ).otherwise(F.col("status"))
    )

    # -------------------------------------------------
    # Final NULL safety for lifecycle fields
    # -------------------------------------------------
    df = (
        df
        .withColumn(
            "status",
            F.coalesce(F.col("status"), F.lit("OPEN"))
        )
        .withColumn(
            "is_valid_claim",
            F.coalesce(F.col("is_valid_claim"), F.lit(False))
        )
    )

    return df
