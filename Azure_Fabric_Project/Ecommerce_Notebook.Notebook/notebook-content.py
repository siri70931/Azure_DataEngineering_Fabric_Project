# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d10eb51f-ad3e-4829-9fb0-2cafdee4ae10",
# META       "default_lakehouse_name": "Ecommerce_Lakehouse",
# META       "default_lakehouse_workspace_id": "485db50e-eadb-46bb-aae0-66de8e8a941c",
# META       "known_lakehouses": [
# META         {
# META           "id": "d10eb51f-ad3e-4829-9fb0-2cafdee4ae10"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Read raw data(Bronze) and create dataframes

# CELL ********************

customer_raw = spark.read.parquet("abfss://Ecommerce_Fabric_Workspace@onelake.dfs.fabric.microsoft.com/Ecommerce_Lakehouse.Lakehouse/Files/Bronze/customers.parquet")
orders_raw = spark.read.parquet("abfss://Ecommerce_Fabric_Workspace@onelake.dfs.fabric.microsoft.com/Ecommerce_Lakehouse.Lakehouse/Files/Bronze/orders.parquet")
payments_raw = spark.read.parquet("abfss://Ecommerce_Fabric_Workspace@onelake.dfs.fabric.microsoft.com/Ecommerce_Lakehouse.Lakehouse/Files/Bronze/payments.parquet")
tickets_raw = spark.read.parquet("abfss://Ecommerce_Fabric_Workspace@onelake.dfs.fabric.microsoft.com/Ecommerce_Lakehouse.Lakehouse/Files/Bronze/support_tickets.parquet")
web_raw = spark.read.parquet("abfss://Ecommerce_Fabric_Workspace@onelake.dfs.fabric.microsoft.com/Ecommerce_Lakehouse.Lakehouse/Files/Bronze/web_activities.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read Bronze Data

# CELL ********************

display(customer_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(orders_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(payments_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(tickets_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(web_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Save Bronze Data as Delta Tables

# CELL ********************

customer_raw.write.format("delta").mode("overwrite").saveAsTable("customers")
orders_raw.write.format("delta").mode("overwrite").saveAsTable("orders")
payments_raw.write.format("delta").mode("overwrite").saveAsTable("payments")
tickets_raw.write.format("delta").mode("overwrite").saveAsTable("tickets")
web_raw.write.format("delta").mode("overwrite").saveAsTable("web")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Clean the data - Silver

# MARKDOWN ********************

# ## Customers_Clean_Data

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_clean = (customer_raw
         .withColumn("email",lower(trim(col("EMAIL"))))
         .withColumn("name",initcap(trim(col("name"))))
         .withColumn("gender",when(lower(col("gender")).isin("f","female"),"Female")
                             .when(lower(col("gender")).isin("m","male"),"Male")
                             .otherwise("other"))
         .withColumn("dob",regexp_replace(col("dob"),"/","-"))
         .withColumn("location",initcap(col("location")))
         .dropDuplicates(["customer_id"])
         .dropna(subset=["customer_id","email"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Orders_Clean_Data

# CELL ********************

orders = spark.read.table("orders")
orders_clean = (orders
            .withColumn("order_date", 
                when(col("order_date").rlike("^\d{4}/\d{2}/\d{2}$"), to_date(col("order_date"), "yyyy/MM/dd"))
                .when(col("order_date").rlike("^\d{2}-\d{2}-\d{4}$"), to_date(col("order_date"), "dd-MM-yyyy"))
                .when(col("order_date").rlike("^\d{8}$"), to_date(col("order_date"), "yyyyMMdd"))
                .otherwise(to_date(col("order_date"), "yyyy-MM-dd")))
            .withColumn("amount", col("amount").cast(DoubleType()))
    .withColumn("amount", when(col("amount") < 0, None).otherwise(col("amount")))
    .withColumn("status", initcap(col("status")))
    .dropna(subset=["customer_id", "order_date"])
    .dropDuplicates(["order_id"]))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Payments_Clean_Data

# CELL ********************

payments = spark.read.table("payments")
payments_clean = (
    payments
    .withColumn("payment_date", to_date(regexp_replace(col("payment_date"), "/", "-")))
    .withColumn("payment_method", initcap(col("payment_method")))
    .replace({"creditcard": "Credit Card"}, subset=["payment_method"])
    .withColumn("payment_status", initcap(col("payment_status")))
    .withColumn("amount", col("amount").cast(DoubleType()))
    .withColumn("amount", when(col("amount") < 0, None).otherwise(col("amount")))
    .dropna(subset=["customer_id", "payment_date", "amount"])
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Tickets_Clean_Data

# CELL ********************

tickets = spark.read.table("tickets")
tickets_clean = (
    tickets
    .withColumn("ticket_date", to_date(regexp_replace(col("ticket_date"), "/", "-")))
    .withColumn("issue_type", initcap(trim(col("issue_type"))))
    .withColumn("resolution_status", initcap(trim(col("resolution_status"))))
    .replace({"NA": None, "": None}, subset=["issue_type", "resolution_status"])
    .dropDuplicates(["ticket_id"])
    .dropna(subset=["customer_id", "ticket_date"])
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Web_Clean_Data

# CELL ********************

web = spark.read.table("web")
web_clean = (
    web
    .withColumn("session_time", to_date(regexp_replace(col("session_time"), "/", "-")))
    .withColumn("page_viewed", lower(col("page_viewed")))
    .withColumn("device_type", initcap(col("device_type")))
    .dropDuplicates(["session_id"])
    .dropna(subset=["customer_id", "session_time", "page_viewed"])
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creating Silver Data as Delta Tables

# CELL ********************

customer_clean.write.format("delta").mode("overwrite").saveAsTable("Silver_customers")
orders_clean.write.format("delta").mode("overwrite").saveAsTable("Silver_Orders")
payments_clean.write.format("delta").mode("overwrite").saveAsTable("Silver_Payments")
tickets_clean.write.format("delta").mode("overwrite").saveAsTable("Silver_Tickets")
web_clean.write.format("delta").mode("overwrite").saveAsTable("Silver_Web")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Final Data - Gold

# CELL ********************

cust_data = spark.read.table("silver_Customers").alias("c")
orders_data = spark.read.table("silver_Orders").alias("o")
payments_data = spark.read.table("silver_Payments").alias("p")
tickets_data = spark.read.table("silver_Tickets").alias("t")
web_data = spark.read.table("silver_Web").alias("w")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_full_data = (
    cust_data
    .join(orders_data, "customer_id", "left")
    .join(payments_data, "customer_id", "left")
    .join(tickets_data, "customer_id", "left")
    .join(web_data, "customer_id", "left")
    .select(
        "c.customer_id", "c.name", "c.email", "c.gender", "c.dob", "c.location",
        "o.order_id", "o.order_date", "o.amount", "o.status",
        "p.payment_method", "p.payment_status",
        "t.ticket_id", "t.issue_type", "t.ticket_date", "t.resolution_status",
        "w.page_viewed", "w.device_type", "w.session_time"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(customer_full_data.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creating Final Data as Delta_Table

# CELL ********************

customer_full_data.write.format("delta").mode("overwrite").saveAsTable("Gold_Customer_Full_Data")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
