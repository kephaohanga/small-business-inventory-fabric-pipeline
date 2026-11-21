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
import random
from datetime import datetime, timedelta
from pyspark.sql import Row

random.seed(42)  # reproducible dataset -- same run produces the same data every time

START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2026, 6, 30)
TOTAL_DAYS = (END_DATE - START_DATE).days

def random_date():
    return START_DATE + timedelta(days=random.randint(0, TOTAL_DAYS))




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# %%
# ---------------------------------------------------------------
# DIMENSIONS -- same shape as the real SBIMS schema, same ID style
# (kept identical to the verified Tier 3 schema so this data would
# load into the same table structure if you ever wanted to).
# ---------------------------------------------------------------

PRODUCTS = [
    ("P001", "Rice 2kg", "Groceries", 250.0, 180.0, 20, 200),
    ("P002", "Cooking Oil 2L", "Groceries", 450.0, 320.0, 15, 150),
    ("P003", "Sugar 1kg", "Groceries", 150.0, 100.0, 25, 250),
    ("P004", "Bread Loaf", "Groceries", 60.0, 40.0, 30, 300),
    ("P005", "Milk 1L", "Groceries", 80.0, 55.0, 40, 400),
    ("P006", "Bar Soap", "Household", 45.0, 28.0, 30, 300),
    ("P007", "Toothpaste", "Household", 120.0, 80.0, 20, 200),
    ("P008", "Detergent 1kg", "Household", 180.0, 130.0, 15, 150),
    ("P009", "Bandages Box", "Pharmacy", 90.0, 55.0, 10, 100),
    ("P010", "Paracetamol Pack", "Pharmacy", 40.0, 22.0, 25, 250),
    ("P011", "Hammer", "Hardware", 600.0, 420.0, 5, 50),
    ("P012", "Nails 1kg", "Hardware", 150.0, 95.0, 10, 100),
    ("P013", "Padlock", "Hardware", 350.0, 220.0, 8, 80),
    ("P014", "Cement 50kg", "Hardware", 900.0, 700.0, 6, 60),
    ("P015", "Exercise Book", "Stationery", 30.0, 18.0, 40, 400),
]

BRANCHES = [("B001", "Main Street"), ("B002", "Market Square"), ("B003", "Estate Branch")]
EMPLOYEES = [("E001","B001"),("E002","B001"),("E003","B002"),("E004","B002"),("E005","B003")]
PAYMENT_METHODS = ["PM001","PM002","PM003","PM004","PM005","PM006"]
SUPPLIERS = [f"SUP{str(i).zfill(3)}" for i in range(1, 11)]
# Expense categories -- each with its OWN realistic amount range and a
# selection weight, instead of one flat random.uniform() shared by every
# category. Without this, a cheap/frequent line item like Packaging draws
# from the exact same 500-15000 range as Rent, so it can randomly come out
# looking as large (or larger) than genuinely big recurring costs -- which
# is not how a real shop's expenses behave and is what made Total Expenses
# land near-parity with Total Revenue in the original dataset (§33 of the
# build guide). Weighted so small, frequent costs (Transport, Packaging)
# show up often but cheap, and big, rare costs (Rent, Wages) show up
# rarely but expensive -- net effect: total Expenses lands around 15-20%
# of Revenue instead of matching it 1:1.
EXPENSE_CATEGORIES = [
    # (ID,     Name,               min_amt, max_amt, weight)
    ("E002", "Rent",               8000, 15000, 0.03),
    ("E003", "Electricity Bill",   1500,  4000, 0.08),
    ("E004", "Salaries",           6000, 12000, 0.05),
    ("E005", "Logistics",           200,  1500, 0.27),
    ("E006", "Packaging",           100,   600, 0.27),
    ("E007", "Repairs",             300,  2500, 0.08),
    ("E008", "Marketing",           500,  3000, 0.08),
    ("E009", "Internet/Data",       300,  1500, 0.10),
    ("E010", "Taxes",              1000,  6000, 0.04),
]
EXPENSE_CATEGORY_WEIGHTS = [c[4] for c in EXPENSE_CATEGORIES]
PURCHASE_STATUSES = ["PS001","PS002","PS003","PS004"]  # Pending/Approved/Posted/Cancelled



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# %%
# ---------------------------------------------------------------
# SALES TRANSACTIONS -- the main volume driver.
# Target: roughly 10x the original 216 rows -> ~2,200 Income rows,
# plus a proportional number of Returns, spread across 18 months
# with a mild seasonal bump (Nov-Dec) so the trend chart looks real.
# ---------------------------------------------------------------

def seasonal_weight(d):
    return 1.6 if d.month in (11, 12) else 1.0

sales_rows = []
income_ids = []
tx_counter = 1

for day_offset in range(TOTAL_DAYS + 1):
    d = START_DATE + timedelta(days=day_offset)
    base_daily_count = int(4 * seasonal_weight(d))
    daily_count = max(0, base_daily_count + random.randint(-2, 3))
    for _ in range(daily_count):
        pid, _, _, price, cost, _, _ = random.choice(PRODUCTS)
        emp, branch = random.choice(EMPLOYEES)
        qty = random.randint(1, 8)
        tx_id = f"T{str(tx_counter).zfill(5)}"
        tx_counter += 1
        income_ids.append((tx_id, d, pid, qty, price, cost, branch, emp))
        sales_rows.append(Row(
            TransactionID=tx_id,
            TransactionDate=d.strftime("%Y-%m-%d"),
            TransactionType="Income",
            ProductID=pid,
            Quantity=qty,
            OriginalTransactionID=None,
            DiscountRate=round(random.choice([0.0, 0.0, 0.0, 0.05, 0.10]), 2),
            PaymentMethod=random.choice(PAYMENT_METHODS),  # raw text/code mix, on purpose -- Silver normalizes this
            BranchID=branch,
            EmployeeID=emp,
            Notes=None,
        ))

# Returns: ~3% of Income rows, referencing a real prior TransactionID
# (never a same-day or earlier-than-original date -- that rule gets enforced in Silver)
for tx_id, d, pid, qty, price, cost, branch, emp in random.sample(income_ids, k=int(len(income_ids) * 0.03)):
    return_date = d + timedelta(days=random.randint(1, 14))
    if return_date > END_DATE:
        continue
    return_id = f"T{str(tx_counter).zfill(5)}"
    tx_counter += 1
    sales_rows.append(Row(
        TransactionID=return_id,
        TransactionDate=return_date.strftime("%Y-%m-%d"),
        TransactionType="Return",
        ProductID=pid,
        Quantity=random.randint(1, qty),
        OriginalTransactionID=tx_id,
        DiscountRate=None,
        PaymentMethod=random.choice(PAYMENT_METHODS),
        BranchID=branch,
        EmployeeID=emp,
        Notes=" Customer return ",  # deliberate leading/trailing whitespace -- Silver trims this
    ))

df_sales_bronze = spark.createDataFrame(sales_rows)
df_sales_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze_sales_transactions")
print(f"bronze_sales_transactions: {df_sales_bronze.count()} rows")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# ---------------------------------------------------------------
# STOCK PURCHASES -- roughly a quarter of the sales row count,
# spread across the same window, always dated before the sales
# that depend on that stock (kept loosely true by spreading evenly).
# ---------------------------------------------------------------

purchase_rows = []
for i in range(1, int(len(income_ids) * 0.25) + 1):
    pid, _, _, price, cost, _, _ = random.choice(PRODUCTS)
    emp, branch = random.choice(EMPLOYEES)
    d = random_date()
    status = random.choices(PURCHASE_STATUSES, weights=[0.1, 0.15, 0.7, 0.05])[0]
    purchase_rows.append(Row(
        PurchaseID=f"PU{str(i).zfill(5)}",
        PurchaseDate=d.strftime("%Y-%m-%d"),
        ProductID=pid,
        Quantity=random.randint(20, 150),
        UnitCost=cost * random.uniform(0.92, 1.08),  # varying batch cost, on purpose
        SupplierName=f"Supplier {random.choice(SUPPLIERS)}",  # raw supplier NAME, not ID -- resolved in Silver/Gold
        PurchaseStatus=status,
        PostedDate=(d + timedelta(days=random.randint(0, 5))).strftime("%Y-%m-%d") if status == "PS003" else None,
        PaymentMethodID=random.choice(PAYMENT_METHODS),
        BranchID=branch,
        EmployeeID=emp,
        Notes=None,
    ))

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

purchases_schema = StructType([
    StructField("PurchaseID", StringType(), True),
    StructField("PurchaseDate", StringType(), True),
    StructField("ProductID", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("UnitCost", DoubleType(), True),
    StructField("SupplierName", StringType(), True),
    StructField("PurchaseStatus", StringType(), True),
    StructField("PostedDate", StringType(), True),
    StructField("PaymentMethodID", StringType(), True),
    StructField("BranchID", StringType(), True),
    StructField("EmployeeID", StringType(), True),
    StructField("Notes", StringType(), True),
])

df_purchases_bronze = spark.createDataFrame(purchase_rows, schema=purchases_schema)
df_purchases_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze_stock_purchases")
print(f"bronze_stock_purchases: {df_purchases_bronze.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# ---------------------------------------------------------------
# STOCK ADJUSTMENTS, EXPENSES, STOCK RECONCILIATION
# Smaller volumes, same 18-month spread, same "raw" idea: minimal
# shaping here, real cleanup happens in Silver.
# ---------------------------------------------------------------

adjustment_rows = []
for i in range(1, int(len(income_ids) * 0.08) + 1):
    pid, _, _, _, _, _, _ = random.choice(PRODUCTS)
    emp, branch = random.choice(EMPLOYEES)
    adjustment_rows.append(Row(
        AdjustmentID=f"ADJ{str(i).zfill(5)}",
        AdjustmentDate=random_date().strftime("%Y-%m-%d"),
        AdjustmentType=random.choice(["Loss", "Damage", "Found", " Loss "]),  # stray whitespace variant, on purpose
        ProductID=pid,
        Quantity=random.randint(1, 10),
        Reason=random.choice(["Breakage", "Theft", "Recount", None]),
        BranchID=branch,
        EmployeeID=emp,
    ))
df_adjustments_bronze = spark.createDataFrame(adjustment_rows)
df_adjustments_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze_stock_adjustments")
print(f"bronze_stock_adjustments: {df_adjustments_bronze.count()} rows")

expense_rows = []
for i in range(1, int(len(income_ids) * 0.12) + 1):
    emp, branch = random.choice(EMPLOYEES)
    cat_id, cat_name, cat_min, cat_max, _ = random.choices(EXPENSE_CATEGORIES, weights=EXPENSE_CATEGORY_WEIGHTS)[0]
    expense_rows.append(Row(
        ExpenseID=f"EXP{str(i).zfill(5)}",
        ExpenseDate=random_date().strftime("%Y-%m-%d"),
        ExpenseCategoryID=cat_id,
        Description=random.choice([cat_name, cat_name, cat_name, None]),  # Description tied to the category actually picked, not an unrelated random label
        Amount=round(random.uniform(cat_min, cat_max), 2),  # category's OWN range, not one flat range shared by every category
        PaymentMethodID=random.choice(PAYMENT_METHODS),
        BranchID=branch,
        EmployeeID=emp,
    ))
df_expenses_bronze = spark.createDataFrame(expense_rows)
df_expenses_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze_expenses")
print(f"bronze_expenses: {df_expenses_bronze.count()} rows")

reconciliation_rows = []
for i in range(1, int(len(income_ids) * 0.03) + 1):
    pid, _, _, _, _, _, _ = random.choice(PRODUCTS)
    emp, branch = random.choice(EMPLOYEES)
    reconciliation_rows.append(Row(
        ReconciliationID=f"RC{str(i).zfill(5)}",
        ReconciliationDate=random_date().strftime("%Y-%m-%d"),
        ProductID=pid,
        PhysicalCount=random.randint(0, 100),
        BranchID=branch if random.random() > 0.1 else None,   # occasional NULL, matching the real nullable design
        EmployeeID=emp if random.random() > 0.1 else None,
        Notes=None,
    ))
reconciliation_schema = StructType([
    StructField("ReconciliationID", StringType(), True),
    StructField("ReconciliationDate", StringType(), True),
    StructField("ProductID", StringType(), True),
    StructField("PhysicalCount", IntegerType(), True),
    StructField("BranchID", StringType(), True),
    StructField("EmployeeID", StringType(), True),
    StructField("Notes", StringType(), True),
])

df_reconciliation_bronze = spark.createDataFrame(reconciliation_rows, schema=reconciliation_schema)
df_reconciliation_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze_stock_reconciliation")
print(f"bronze_stock_reconciliation: {df_reconciliation_bronze.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# ---------------------------------------------------------------
# DIMENSION TABLES -- landed as-is too, for consistency, even
# though they need almost no cleaning (kept in Bronze anyway so
# every table in Silver/Gold has a real Bronze source to point back to).
# ---------------------------------------------------------------

df_products = spark.createDataFrame(
    [Row(ProductID=p[0], ProductName=p[1], Category=p[2], UnitPrice=p[3], UnitCost=p[4],
         ReorderLevel=p[5], MaxStock=p[6]) for p in PRODUCTS]
)
df_products.write.format("delta").mode("overwrite").saveAsTable("bronze_products")

df_branches = spark.createDataFrame([Row(BranchID=b[0], BranchName=b[1]) for b in BRANCHES])
df_branches.write.format("delta").mode("overwrite").saveAsTable("bronze_branches")

df_employees = spark.createDataFrame([Row(EmployeeID=e[0], BranchID=e[1]) for e in EMPLOYEES])
df_employees.write.format("delta").mode("overwrite").saveAsTable("bronze_employees")

print("Bronze layer complete.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
