# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9272083c-1dc5-4d21-9066-90ca697780f7",
# META       "default_lakehouse_name": "SBIMS_Lakehouse",
# META       "default_lakehouse_workspace_id": "863ee5c1-c7b1-4fb2-9104-549f29dc1eff",
# META       "known_lakehouses": [
# META         {
# META           "id": "9272083c-1dc5-4d21-9066-90ca697780f7"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "2f916f2a-0016-4fc5-b690-ac99f44cd208",
# META       "known_warehouses": [
# META         {
# META           "id": "2f916f2a-0016-4fc5-b690-ac99f44cd208",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# ---------------------------------------------------------------
# GOLD DIMENSIONS -- straight promotions from Silver, renamed with
# a gold_dim_ prefix so the model's fact/dimension split is obvious
# to anyone opening the Lakehouse (mirrors the star-schema naming
# convention from the Power BI walkthrough).
# ---------------------------------------------------------------

spark.table("silver_products").write.format("delta").mode("overwrite").saveAsTable("gold_dim_products")
spark.table("silver_branches").write.format("delta").mode("overwrite").saveAsTable("gold_dim_branches")
spark.table("silver_employees").write.format("delta").mode("overwrite").saveAsTable("gold_dim_employees")

# Payment methods dimension -- built here since Bronze never had one as its own table
df_payment_methods = spark.createDataFrame(
    [("PM001", "Cash"), ("PM002", "Card"), ("PM003", "Mobile Money"),
     ("PM004", "Bank Transfer"), ("PM005", "Check"), ("PM006", "Other")],
    ["PaymentMethodID", "PaymentMethodName"]
)
df_payment_methods.write.format("delta").mode("overwrite").saveAsTable("gold_dim_payment_methods")

# Suppliers dimension -- resolved from the free-text SupplierName seen in
# Silver.stock_purchases, the same "mint an ID from a distinct name" problem
# Suppliers.dtsx solved with a Script Component; here it's a simple dense_rank.
df_supplier_names = spark.table("silver_stock_purchases").select("SupplierName").distinct()
df_suppliers = df_supplier_names.withColumn(
    "SupplierID",
    F.concat(F.lit("SUP"), F.lpad(F.dense_rank().over(Window.orderBy("SupplierName")).cast("string"), 3, "0"))
).select("SupplierID", "SupplierName")
df_suppliers.write.format("delta").mode("overwrite").saveAsTable("gold_dim_suppliers")

# Date dimension -- required for time-intelligence measures in Power BI,
# same reasoning as the CALENDAR() DAX table from the semantic-model walkthrough
df_dates = spark.sql("""
    SELECT explode(sequence(to_date('2025-01-01'), to_date('2026-06-30'), interval 1 day)) AS Date
""").withColumn("Year", F.year("Date")) \
   .withColumn("MonthNumber", F.month("Date")) \
   .withColumn("MonthName", F.date_format("Date", "MMMM")) \
   .withColumn("Quarter", F.quarter("Date"))
df_dates.write.format("delta").mode("overwrite").saveAsTable("gold_dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# ---------------------------------------------------------------
# GOLD FACT: SALES -- the equivalent of vw_SalesTransactionsDetail.
# Resolves the Return self-reference (pulling the original Income
# row's price/cost via a self-join) and computes Revenue, Cost,
# GrossProfit, RefundAmount as real stored columns here -- Gold is
# meant to be pre-computed, so Power BI's measures stay simple SUMs.
# ---------------------------------------------------------------

df_sales = spark.table("silver_sales_transactions")
df_products_g = spark.table("gold_dim_products").select("ProductID", "UnitPrice", "UnitCost")

df_income = df_sales.filter(F.col("TransactionType") == "Income").join(df_products_g, "ProductID")
df_income = df_income.withColumn("Revenue", F.round(F.col("Quantity") * F.col("UnitPrice") * (1 - F.coalesce(F.col("DiscountRate"), F.lit(0))), 2)) \
                      .withColumn("Cost", F.round(F.col("Quantity") * F.col("UnitCost"), 2)) \
                      .withColumn("GrossProfit", F.round(F.col("Revenue") - F.col("Cost"), 2)) \
                      .withColumn("RefundAmount", F.lit(0.0))

# Returns pull the ORIGINAL row's per-unit price/cost, then negate --
# same logic vw_SalesTransactionsDetail_Amounts implements in SQL
df_income_lookup = df_income.select(
    F.col("TransactionID").alias("OrigID"),
    (F.col("Revenue") / F.col("Quantity")).alias("OrigUnitRevenue"),
    (F.col("Cost") / F.col("Quantity")).alias("OrigUnitCost"),
)

df_returns = df_sales.filter(F.col("TransactionType") == "Return") \
    .join(df_income_lookup, df_sales.OriginalTransactionID == df_income_lookup.OrigID, "left") \
    .withColumn("Revenue", F.round(-F.col("Quantity") * F.col("OrigUnitRevenue"), 2)) \
    .withColumn("Cost", F.round(-F.col("Quantity") * F.col("OrigUnitCost"), 2)) \
    .withColumn("GrossProfit", F.round(F.col("Revenue") - F.col("Cost"), 2)) \
    .withColumn("RefundAmount", F.round(-F.col("Revenue"), 2)) \
    .drop("OrigID", "OrigUnitRevenue", "OrigUnitCost")

common_cols = ["TransactionID", "TransactionDate", "TransactionType", "ProductID", "Quantity",
               "OriginalTransactionID", "DiscountRate", "PaymentMethod", "BranchID", "EmployeeID",
               "Notes", "Revenue", "Cost", "GrossProfit", "RefundAmount"]

df_fact_sales = df_income.select(*common_cols).unionByName(df_returns.select(*common_cols))
df_fact_sales.write.format("delta").mode("overwrite").saveAsTable("gold_fact_sales")
print(f"gold_fact_sales: {df_fact_sales.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# ---------------------------------------------------------------
# GOLD FACT: STOCK MOVEMENT -- the equivalent of the InventoryLedger
# view's window function. Combines purchases (+), sales (-), returns
# (+), and adjustments (+/-) into one running-balance ledger per product.
# ---------------------------------------------------------------

purchases_mv = spark.table("silver_stock_purchases").filter(F.col("PurchaseStatus") == "PS003") \
    .select(F.col("PurchaseID").alias("ReferenceID"), F.col("PurchaseDate").alias("MovementDate"),
            "ProductID", F.col("Quantity").alias("StockMovement"), F.lit("Purchase").alias("MovementType"))

sales_mv = df_fact_sales.withColumn(
        "StockMovement", F.when(F.col("TransactionType") == "Income", -F.col("Quantity")).otherwise(F.col("Quantity"))
    ).select(F.col("TransactionID").alias("ReferenceID"), F.col("TransactionDate").alias("MovementDate"),
             "ProductID", "StockMovement", F.col("TransactionType").alias("MovementType"))

adjustments_mv = spark.table("silver_stock_adjustments").withColumn(
        "StockMovement", F.when(F.col("AdjustmentType") == "Found", F.col("Quantity")).otherwise(-F.col("Quantity"))
    ).select(F.col("AdjustmentID").alias("ReferenceID"), F.col("AdjustmentDate").alias("MovementDate"),
             "ProductID", "StockMovement", F.col("AdjustmentType").alias("MovementType"))

df_movements = purchases_mv.unionByName(sales_mv).unionByName(adjustments_mv)

running_window = Window.partitionBy("ProductID").orderBy("MovementDate", "ReferenceID").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_fact_stock_ledger = df_movements.withColumn("BalanceAfterMovement", F.sum("StockMovement").over(running_window))

df_fact_stock_ledger.write.format("delta").mode("overwrite").saveAsTable("gold_fact_stock_ledger")
print(f"gold_fact_stock_ledger: {df_fact_stock_ledger.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# ---------------------------------------------------------------
# GOLD FACT: STOCK PURCHASES, EXPENSES, STOCK RECONCILIATION
# Passed through with the SupplierName -> SupplierID resolution
# applied to purchases (Gold is where that translation belongs,
# same reasoning as Suppliers.dtsx's Lookup step in the SSIS project).
# ---------------------------------------------------------------

df_suppliers_lookup = spark.table("gold_dim_suppliers")
df_fact_purchases = spark.table("silver_stock_purchases").join(df_suppliers_lookup, "SupplierName").drop("SupplierName")
df_fact_purchases.write.format("delta").mode("overwrite").saveAsTable("gold_fact_stock_purchases")

spark.table("silver_expenses").write.format("delta").mode("overwrite").saveAsTable("gold_fact_expenses")
spark.table("silver_stock_adjustments").write.format("delta").mode("overwrite").saveAsTable("gold_fact_stock_adjustments")
spark.table("silver_stock_reconciliation").write.format("delta").mode("overwrite").saveAsTable("gold_fact_stock_reconciliation")

df_exp_cat = spark.createDataFrame(
    [("E002", "Rent",             "Shop rent"),
     ("E003", "Electricity Bill", "Power/utility payments"),
     ("E004", "Salaries",         "Staff wages and salaries"),
     ("E005", "Logistics",        "Delivery and transport costs"),
     ("E006", "Packaging",        "Bags, labels, cartons"),
     ("E007", "Repairs",          "Equipment and shop repairs"),
     ("E008", "Marketing",        "Promotions and adverts"),
     ("E009", "Internet/Data",    "Internet or mobile data"),
     ("E010", "Taxes",            "Business taxes, licenses, and government fees")],
    ["ExpenseCategoryID", "ExpenseCategory", "CategoryDescription"]
)
df_exp_cat.write.format("delta").mode("overwrite").saveAsTable("gold_dim_expense_categories")

print("Gold layer complete. Connect Power BI to the gold_* tables via DirectLake.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
total_revenue = spark.table("gold_fact_sales").agg(F.sum("Revenue")).collect()[0][0]
total_expenses = spark.table("gold_fact_expenses").agg(F.sum("Amount")).collect()[0][0]
print(f"Total Revenue:  {total_revenue:,.2f}")
print(f"Total Expenses: {total_expenses:,.2f}")
print(f"Expenses as % of Revenue: {total_expenses/total_revenue*100:.1f}%")

spark.table("gold_fact_expenses") \
    .groupBy("ExpenseCategoryID") \
    .agg(F.sum("Amount").alias("TotalAmount")) \
    .orderBy(F.desc("TotalAmount")) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
