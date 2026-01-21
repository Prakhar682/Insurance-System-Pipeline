# --------------------------------
# GOLD
# --------------------------------


import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dlt.table(
    name="kpi_loss_ratio",
    comment="Gold KPI: Monthly loss ratio with risk flag and NULL-safe month handling"
)
def kpi_loss_ratio():

    # --------------------------------
    # Read Silver tables
    # --------------------------------
    policies = dlt.read("fact_policy_risk_profile")
    claims = dlt.read("fact_claims_lifecycle")

    # --------------------------------
    # Premiums collected (monthly)
    # --------------------------------
    premiums = (
        policies
        .filter(F.col("start_date").isNotNull())
        .filter(F.col("premium_amount").isNotNull())
        .withColumn("month", F.date_trunc("month", F.col("start_date")))
        .groupBy("month")
        .agg(
            F.sum("premium_amount").alias("total_premiums_collected")
        )
    )

    # --------------------------------
    # Claims paid (CLOSED only)
    # --------------------------------
    claims_paid = (
        claims
        .filter(F.col("status") == "CLOSED")
        .filter(F.col("last_payment_date").isNotNull())
        .withColumn("month", F.date_trunc("month", F.col("last_payment_date")))
        .groupBy("month")
        .agg(
            F.sum("total_amount_paid").alias("total_claims_paid")
        )
    )

    # --------------------------------
    # Claims reserved (OPEN + REOPENED)
    # --------------------------------
    claims_reserved = (
        claims
        .filter(F.col("status").isin("OPEN", "REOPENED"))
        .filter(F.col("filed_date").isNotNull())
        .withColumn("month", F.date_trunc("month", F.col("filed_date")))
        .groupBy("month")
        .agg(
            F.sum("claim_amount").alias("claims_reserved")
        )
    )

    # --------------------------------
    # Combine all metrics
    # --------------------------------
    df = (
        premiums
        .join(claims_paid, "month", "left")
        .join(claims_reserved, "month", "left")
        .fillna({
            "total_claims_paid": 0,
            "claims_reserved": 0
        })
    )

    # --------------------------------
    # Loss ratio calculation (safe)
    # --------------------------------
    df = df.withColumn(
        "loss_ratio_percentage",
        F.when(
            F.col("total_premiums_collected") > 0,
            ((F.col("total_claims_paid") + F.col("claims_reserved")) /
             F.col("total_premiums_collected")) * 100
        ).otherwise(0)
    )

    # --------------------------------
    # Risk interpretation flag
    # --------------------------------
    df = df.withColumn(
        "loss_ratio_flag",
        F.when(F.col("loss_ratio_percentage") > 300, "EXTREME")
         .when(F.col("loss_ratio_percentage") > 150, "HIGH")
         .otherwise("NORMAL")
    )

    # --------------------------------
    # FINAL GOLD RULE: no NULL month
    # --------------------------------
    df = df.filter(F.col("month").isNotNull())

    return df


@dlt.table(
    name="kpi_claim_processing_lag",
    comment="Gold KPI: monthly claim processing lag (NULL & negative safe)"
)
def kpi_claim_processing_lag():

    cl = (
        dlt.read("fact_claims_lifecycle")
        .filter(F.col("status") == "CLOSED")
        .filter(F.col("filed_date").isNotNull())
        .filter(F.col("last_payment_date").isNotNull())
    )

    # ----------------------------------------
    # Calculate processing days
    # ----------------------------------------
    df = cl.withColumn(
        "processing_days",
        F.datediff(F.col("last_payment_date"), F.col("filed_date"))
    ).filter(
        F.col("processing_days") >= 0
    )

    # ----------------------------------------
    # Month bucket
    # ----------------------------------------
    df = df.withColumn(
        "month",
        F.date_trunc("month", F.col("filed_date"))
    )

    # ----------------------------------------
    # Aggregate KPIs
    # ----------------------------------------
    agg = df.groupBy("month").agg(
        F.count("*").alias("total_claims_processed"),
        F.avg("processing_days").alias("avg_processing_days"),
        F.expr("percentile_approx(processing_days, 0.5)")
            .alias("median_processing_days"),
        F.sum(F.when(F.col("processing_days") <= 30, 1).otherwise(0))
            .alias("claims_processed_within_30_days"),
        F.sum(F.when(F.col("processing_days") > 60, 1).otherwise(0))
            .alias("claims_processed_over_60_days")
    )

    # ----------------------------------------
    # Final NULL safety (dashboard-ready)
    # ----------------------------------------
    return agg.fillna({
        "total_claims_processed": 0,
        "avg_processing_days": 0,
        "median_processing_days": 0,
        "claims_processed_within_30_days": 0,
        "claims_processed_over_60_days": 0
    })

@dlt.table(
    name="kpi_premium_leakage_analysis",
    comment="Premium leakage detection for high risk customers"
)
def kpi_premium_leakage_analysis():

    p = dlt.read("fact_policy_risk_profile").alias("p")
    c = dlt.read("dim_customers").filter("is_current = true").alias("c")

    df = (
        p
        .filter((F.col("end_date").isNull()) | (F.col("end_date") > F.current_date()))
        .join(c, "customer_id", "inner")
    )

    #  percentile as window expression (SAFE)
    w = Window.partitionBy("policy_type")

    df = df.withColumn(
        "premium_25th_percentile",
        F.expr("percentile_approx(premium_amount, 0.25)").over(w)
    )

    df = df.withColumn(
        "expected_premium_midpoint",
        F.when(F.col("risk_category") == "LOW", 400)
         .when(F.col("risk_category") == "MEDIUM", 650)
         .otherwise(1150)
    )

    df = df.withColumn(
        "premium_leakage_amount",
        F.col("expected_premium_midpoint") - F.col("premium_amount")
    )

    df = df.filter(
        ((F.col("risk_category") == "HIGH") &
         (F.col("premium_amount") <= F.col("premium_25th_percentile"))) |
        ((F.col("risk_score") > 5) & (F.col("premium_amount") < 500))
    )

    return df.select(
        "policy_id",
        "customer_id",
        "risk_score",
        "risk_category",
        "premium_amount",
        "premium_leakage_amount",
        "expected_premium_midpoint",
        F.when(F.col("premium_leakage_amount") > 0,
               "Increase Premium on Renewal")
         .otherwise("Review Policy")
         .alias("recommendation")
    )

@dlt.table(
    name="kpi_frequency_severity",
    comment="Gold KPI: Claim frequency & severity by policy type (clean, NULL-safe)"
)
def kpi_frequency_severity():

    policies = dlt.read("fact_policy_risk_profile")
    claims = dlt.read("fact_claims_lifecycle")

    # --------------------------------
    # VALID POLICIES (no NULL dims)
    # --------------------------------
    policies_monthly = (
        policies
        .filter(F.col("start_date").isNotNull())
        .filter(F.col("policy_type").isNotNull())
        .withColumn("month", F.date_trunc("month", F.col("start_date")))
        .groupBy("month", "policy_type")
        .agg(
            F.countDistinct("policy_id").alias("total_policies")
        )
    )

    # --------------------------------
    # VALID CLAIMS (only valid + dated)
    # --------------------------------
    claims_monthly = (
        claims
        .filter(F.col("is_valid_claim") == True)
        .filter(F.col("incident_date").isNotNull())
        .filter(F.col("policy_type").isNotNull())
        .withColumn("month", F.date_trunc("month", F.col("incident_date")))
        .groupBy("month", "policy_type")
        .agg(
            F.count("*").alias("total_claims"),
            F.avg("claim_amount").alias("avg_claim_amount"),
            F.expr("percentile_approx(claim_amount, 0.5)")
                .alias("median_claim_amount")
        )
    )

    # --------------------------------
    # JOIN + NULL-SAFE METRICS
    # --------------------------------
    df = (
        policies_monthly
        .join(claims_monthly, ["month", "policy_type"], "left")
        .fillna({
            "total_claims": 0,
            "avg_claim_amount": 0,
            "median_claim_amount": 0
        })
    )

    # --------------------------------
    # CLAIMS PER 1000 POLICIES (SAFE)
    # --------------------------------
    df = df.withColumn(
        "claims_per_1000_policies",
        F.when(
            F.col("total_policies") > 0,
            (F.col("total_claims") / F.col("total_policies")) * 1000
        ).otherwise(0)
    )

    # --------------------------------
    # FINAL GOLD RULES
    # --------------------------------
    df = (
        df
        .filter(F.col("month").isNotNull())
        .filter(F.col("policy_type").isNotNull())
    )

    return df
