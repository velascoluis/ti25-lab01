from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import logging
from datetime import datetime


def calculate_delinquency_rates():
    # Configure logging to only show WARN and above
    logging.getLogger("py4j").setLevel(logging.WARN)

    sc = SparkContext.getOrCreate()
    sqlContext = HiveContext(sc)

    # Set Spark's log level to WARN
    sc.setLogLevel("WARN")

    hdfs_base_path = "hdfs://quickstart.cloudera:8020/user/hive/warehouse/ccf_db.db"
    loan_apps_df = sqlContext.read.parquet(hdfs_base_path + "/loan_applications")
    repayments_df = sqlContext.read.parquet(hdfs_base_path + "/loan_repayments")
    customers_df = sqlContext.read.parquet(hdfs_base_path + "/customers")

    print("\nNumber of customers: " + str(customers_df.count()))
    print("Number of loan applications: " + str(loan_apps_df.count()))
    print("Number of repayments: " + str(repayments_df.count()) + "\n")

    approved_loans = loan_apps_df.filter("application_status = 'Approved'")

    loan_repayments = (
        repayments_df.join(
            approved_loans, repayments_df.loan_id == approved_loans.application_id
        )
        .join(customers_df, approved_loans.customer_id == customers_df.customer_id)
        .select(
            repayments_df["*"],
            approved_loans["product_type"],
            customers_df["first_name"],
            customers_df["last_name"],
            customers_df["customer_id"].alias("customer_id"),
        )
    )

    # Verify join results
    print("Number of records after joins: " + str(loan_repayments.count()) + "\n")

    # Categorize delinquency buckets - using older syntax
    delinquent_loans = loan_repayments.withColumn(
        "delinquency_bucket",
        F.when(F.col("days_past_due") == 0, "Current")
        .when(F.col("days_past_due").between(1, 29), "1-29 Days")
        .when(F.col("days_past_due").between(30, 59), "30-59 Days")
        .when(F.col("days_past_due").between(60, 89), "60-89 Days")
        .when(F.col("days_past_due") >= 90, "90+ Days")
        .otherwise("Current"),
    )

    # Calculate totals by product type
    product_totals = delinquent_loans.groupBy("product_type").agg(
        F.count("*").alias("total_payments")
    )

    # Calculate delinquencies by product type and bucket
    delinquency_counts = delinquent_loans.groupBy(
        "product_type", "delinquency_bucket"
    ).agg(F.count("*").alias("count"))

    # Calculate rates
    delinquency_rates = delinquency_counts.join(product_totals, "product_type")
    delinquency_rates = delinquency_rates.withColumn(
        "delinquency_rate", F.round(F.col("count") / F.col("total_payments") * 100, 2)
    )

    # Get top 3 delinquent customers
    window_spec = Window.partitionBy("product_type", "delinquency_bucket").orderBy(
        F.desc("days_past_due")
    )

    # Add customer details to delinquent records
    top_delinquents = (
        delinquent_loans.filter(F.col("days_past_due") > 0)
        .select(
            "product_type",
            "delinquency_bucket",
            "days_past_due",
            "first_name",
            "last_name",
            "customer_id",
        )
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") <= 3)
        .withColumn(
            "customer_name",
            F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")),
        )
    )

    # Cache the results
    top_delinquents.cache()

    # Format and display results
    print("\nDelinquency Rates by Product Type:")
    print("===================================")

    results = delinquency_rates.orderBy("product_type", "delinquency_bucket").collect()
    top_customers = top_delinquents.orderBy(
        "product_type", "delinquency_bucket", "rank"
    ).collect()

    current_product = None

    # Convert results to a DataFrame for Hive storage
    csv_data = []
    for row in results:
        base_row = {
            "product_type": row.product_type,
            "delinquency_bucket": row.delinquency_bucket,
            "delinquency_rate": row.delinquency_rate,
        }

        # Add top delinquent customers if applicable
        if row.delinquency_bucket != "Current":
            matching_customers = [
                cust
                for cust in top_customers
                if cust.product_type == row.product_type
                and cust.delinquency_bucket == row.delinquency_bucket
            ]
            for i, cust in enumerate(matching_customers, 1):
                cust_num = "customer_" + str(i) + "_"
                base_row[cust_num + "name"] = cust.customer_name
                base_row[cust_num + "id"] = cust.customer_id
                base_row[cust_num + "days_past_due"] = cust.days_past_due

        csv_data.append(base_row)

    # Save results to HDFS as parquet
    output_path = hdfs_base_path + "/delinquency_results"

    output_df = sqlContext.createDataFrame(csv_data)
    output_df.write.mode("overwrite").parquet(output_path)

    print("\nResults saved to HDFS: " + output_path)

    for row in results:
        if current_product != row.product_type:
            print("\n" + row.product_type)
            print("-" * len(row.product_type))
            current_product = row.product_type

        print(row.delinquency_bucket + ": " + str(row.delinquency_rate) + "%")

        if row.delinquency_bucket != "Current":
            print("  Top 3 delinquent customers:")
            matching_customers = [
                cust
                for cust in top_customers
                if cust.product_type == row.product_type
                and cust.delinquency_bucket == row.delinquency_bucket
            ]
            for cust in matching_customers:
                print(
                    "    "
                    + str(cust.rank)
                    + ". "
                    + cust.customer_name
                    + " (Customer ID: "
                    + str(cust.customer_id)
                    + ", "
                    + str(cust.days_past_due)
                    + " days past due)"
                )

    # Unpersist cached data and stop SparkContext
    top_delinquents.unpersist()
    sc.stop()


if __name__ == "__main__":
    calculate_delinquency_rates()
