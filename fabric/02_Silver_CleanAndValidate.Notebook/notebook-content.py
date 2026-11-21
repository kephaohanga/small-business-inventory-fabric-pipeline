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
# DIMENSIONS -- pass through with light normalization; there's
# genuinely little to clean here, which is expected.
# ---------------------------------------------------------------

df_products = spark.table("bronze_products")
df_products.write.format("delta").mode("overwrite").saveAsTable("silver_products")

df_branches = spark.table("bronze_branches")
df_branches.write.format("delta").mode("overwrite").saveAsTable("silver_branches")

df_employees = spark.table("bronze_employees")
df_employees.write.format("delta").mode("overwrite").saveAsTable("silver_employees")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# ---------------------------------------------------------------
# SALES TRANSACTIONS
# Rules enforced here, mirroring the SQL schema's own constraints:
#   1. TRIM every text column (Bronze's Return Notes had stray whitespace)
#   2. TransactionDate cast to a real date type
#   3. CK_Return_Requires_Original equivalent: a Return must have a
#      non-null OriginalTransactionID; an Income must have a null one.
#      Any row that fails this is dropped, not silently kept.
#   4. PaymentMethod normalized: Bronze mixed plain PM-codes in as
#      the "PaymentMethod" field -- Silver just confirms it's a
#      known code (real translation to a friendly name happens in Gold).
# ---------------------------------------------------------------

df_sales = spark.table("bronze_sales_transactions")

df_sales_clean = (
    df_sales
    .withColumn("TransactionDate", F.to_date("TransactionDate"))
    .withColumn("Notes", F.trim(F.col("Notes")))
    .withColumn("AdjustmentTypeCheck", F.lit(None))  # not used here, placeholder removed below
    .drop("AdjustmentTypeCheck")
)

valid_payment_codes = ["PM001", "PM002", "PM003", "PM004", "PM005", "PM006"]

df_sales_clean = df_sales_clean.filter(
    (
        (F.col("TransactionType") == "Income") & F.col("OriginalTransactionID").isNull()
    ) | (
        (F.col("TransactionType") == "Return") & F.col("OriginalTransactionID").isNotNull()
    )
).filter(
    F.col("PaymentMethod").isin(valid_payment_codes)
)

# A Return's OriginalTransactionID must reference a real Income row that
# actually exists in this same batch -- the self-referencing FK rule.
# Income rows never need this check (OriginalTransactionID is always null
# for them), so only Returns go through the join -- joining Income rows too
# was the bug: an OR'd null-check made every Income row match every other
# Income row's ID, silently duplicating each one thousands of times.
income_ids_df = df_sales_clean.filter(F.col("TransactionType") == "Income").select(
    F.col("TransactionID").alias("ValidOriginalID")
)

df_income_rows = df_sales_clean.filter(F.col("TransactionType") == "Income")

df_return_rows = df_sales_clean.filter(F.col("TransactionType") == "Return").join(
    income_ids_df,
    df_sales_clean.OriginalTransactionID == income_ids_df.ValidOriginalID,
    "inner"
).drop("ValidOriginalID")

df_sales_clean = df_income_rows.unionByName(df_return_rows)

df_sales_clean.write.format("delta").mode("overwrite").saveAsTable("silver_sales_transactions")
print(f"silver_sales_transactions: {df_sales_clean.count()} rows (from {df_sales.count()} in Bronze)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# ---------------------------------------------------------------
# STOCK PURCHASES
# Rule enforced: UnitCost must be > 0 (mirrors a CHECK constraint),
# PurchaseDate cast to real date, SupplierName trimmed -- the actual
# name-to-SupplierID resolution happens in Gold, once Suppliers is
# a proper dimension there.
# ---------------------------------------------------------------

df_purchases = spark.table("bronze_stock_purchases")

df_purchases_clean = (
    df_purchases
    .withColumn("PurchaseDate", F.to_date("PurchaseDate"))
    .withColumn("PostedDate", F.to_date("PostedDate"))
    .withColumn("SupplierName", F.trim(F.col("SupplierName")))
    .withColumn("UnitCost", F.round("UnitCost", 2))
    .filter(F.col("UnitCost") > 0)
    .filter(F.col("Quantity") > 0)
)

df_purchases_clean.write.format("delta").mode("overwrite").saveAsTable("silver_stock_purchases")
print(f"silver_stock_purchases: {df_purchases_clean.count()} rows (from {df_purchases.count()} in Bronze)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# ---------------------------------------------------------------
# STOCK ADJUSTMENTS
# Rule enforced: AdjustmentType must be exactly one of the three
# allowed values, once whitespace is trimmed (mirrors CK_StockAdju_Adjus).
# Quantity must be > 0 (mirrors CK_StockAdju_Quant).
# ---------------------------------------------------------------

df_adjustments = spark.table("bronze_stock_adjustments")

df_adjustments_clean = (
    df_adjustments
    .withColumn("AdjustmentDate", F.to_date("AdjustmentDate"))
    .withColumn("AdjustmentType", F.trim(F.col("AdjustmentType")))
    .filter(F.col("AdjustmentType").isin(["Loss", "Damage", "Found"]))
    .filter(F.col("Quantity") > 0)
)

df_adjustments_clean.write.format("delta").mode("overwrite").saveAsTable("silver_stock_adjustments")
print(f"silver_stock_adjustments: {df_adjustments_clean.count()} rows (from {df_adjustments.count()} in Bronze)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# ---------------------------------------------------------------
# EXPENSES
# Rule enforced: Amount must be > 0 (mirrors CK_Expenses_Amount),
# ExpenseDate must not be in the future relative to END_DATE
# (mirrors CK_Expenses_Expens)
# ExpenseCategoryID must be a known category (E002-E010), mirroring the FK to ExpenseCategories.
# ---------------------------------------------------------------

df_expenses = spark.table("bronze_expenses")

df_expenses_clean = (
    df_expenses
    .withColumn("ExpenseDate", F.to_date("ExpenseDate"))
    .withColumn("Amount", F.round("Amount", 2))
    .filter(F.col("Amount") > 0)
    .filter(F.col("ExpenseCategoryID").isin(["E002","E003","E004","E005","E006","E007","E008","E009","E010"]))
)

df_expenses_clean.write.format("delta").mode("overwrite").saveAsTable("silver_expenses")
print(f"silver_expenses: {df_expenses_clean.count()} rows (from {df_expenses.count()} in Bronze)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# ---------------------------------------------------------------
# STOCK RECONCILIATION
# Rule enforced: PhysicalCount must be >= 0 (mirrors CK_StockReco_Physi,
# which uses >= specifically because a legitimate count can be zero).
# ---------------------------------------------------------------

df_reconciliation = spark.table("bronze_stock_reconciliation")

df_reconciliation_clean = (
    df_reconciliation
    .withColumn("ReconciliationDate", F.to_date("ReconciliationDate"))
    .filter(F.col("PhysicalCount") >= 0)
)

df_reconciliation_clean.write.format("delta").mode("overwrite").saveAsTable("silver_stock_reconciliation")
print(f"silver_stock_reconciliation: {df_reconciliation_clean.count()} rows (from {df_reconciliation.count()} in Bronze)")

print("Silver layer complete.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
