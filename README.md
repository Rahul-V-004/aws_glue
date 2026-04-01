# E-Commerce Sales ETL Pipeline — AWS S3 → Glue → S3 (Parquet) → Athena

A serverless ETL pipeline that fetches real product data from the **[DummyJSON API](https://dummyjson.com/)** (free, no API key), generates e-commerce orders, uploads CSV to **S3**, uses an **AWS Glue Crawler** to catalog the data, runs a **Glue ETL Job** (PySpark) to transform and enrich it, and writes optimized **Parquet** output back to S3 — queryable with **Athena**.

## Architecture

```
DummyJSON API                     AWS Cloud
┌──────────┐      ┌──────────────────────────────────────────────────────────┐
│  Products │      │                                                          │
│  & Users  │      │  ┌──────────┐  Crawler   ┌──────────────┐              │
│  (JSON)   │─────▶│  │ S3 Bucket│──────────▶│  Glue Data   │              │
│           │ CSV  │  │ /raw-data│           │  Catalog     │              │
└──────────┘      │  └──────────┘           └──────┬───────┘              │
                   │                                 │                      │
  fetch_data.py    │                          ┌──────▼───────┐             │
  (local script)   │                          │  Glue ETL    │             │
                   │                          │  Job (Spark) │             │
                   │                          └──────┬───────┘             │
                   │                                 │                      │
                   │              ┌──────────────────┼──────────────┐      │
                   │              ▼                   ▼              ▼      │
                   │     ┌──────────────┐  ┌──────────────┐  ┌──────────┐ │
                   │     │  Enriched     │  │  Daily       │  │ Product  │ │
                   │     │  Orders       │  │  Summary     │  │ Summary  │ │
                   │     │  (Parquet)    │  │  (Parquet)   │  │ (Parquet)│ │
                   │     └──────────────┘  └──────────────┘  └──────────┘ │
                   │              │                                         │
                   │              ▼                                         │
                   │     ┌──────────────┐                                  │
                   │     │   Athena      │ ← Query with SQL                │
                   │     └──────────────┘                                  │
                   └──────────────────────────────────────────────────────────┘
```

## What the Pipeline Does

### 1. **Fetch** (DummyJSON API → CSV → S3)
- Fetches **real product data** (50 products with names, prices, categories, brands, ratings) from `dummyjson.com/products`
- Fetches **real user data** (30 users with names, cities) from `dummyjson.com/users`
- Generates realistic orders around those products (configurable count, default 5000)
- Saves CSV locally and uploads to `s3://<bucket>/raw-data/sales/`

### 2. **Crawl** (Glue Crawler → Data Catalog)
- Glue Crawler scans the CSV files in S3
- Automatically infers the schema (column names, types)
- Registers the table in the **Glue Data Catalog** (like a Hive metastore)

### 3. **Transform** (Glue ETL Job — PySpark)
- Reads data from the Glue Data Catalog
- Casts string fields to proper numeric types
- Adds derived columns:
  - `order_year`, `order_month`, `order_day`, `day_name` — date parts
  - `price_tier` — Budget / Mid-Range / Premium / Luxury
  - `order_size` — Single / Small / Bulk
  - `rating_tier` — Top Rated / Well Rated / Average / Below Average
  - `has_discount` — boolean flag
  - `profit_estimate` — 30% of total amount
- Produces **4 output datasets** as Parquet:
  - **Enriched Orders** — all orders with derived fields, partitioned by `category` and `order_status`
  - **Daily Sales Summary** — aggregated by date, category, region
  - **Category Performance** — revenue, avg order value, unique customers per category
  - **Product Performance** — per-product stats with API ratings and brands

### 4. **Query** (Athena)
- Run a second Glue Crawler on the Parquet output (or create tables manually)
- Query the processed data with standard SQL in Athena

## Data Source

Uses the **[DummyJSON API](https://dummyjson.com/)** — a free REST API for prototyping:
- **No API key required**
- `/products` — 50 real-looking products across 20+ categories (electronics, beauty, furniture, groceries, clothing, etc.)
- `/users` — 30 users with names, addresses, and cities
- Each product has a real name, price, brand, category, rating, and stock info

## Prerequisites

- An **AWS account** (Free Tier eligible — [sign up](https://aws.amazon.com/))
- **Python 3.11+** with `boto3` installed on your local machine
- **AWS CLI** installed and configured (for uploading data from your machine)

## Project Structure

```
aws-glue-pipeline/
├── fetch_data.py              # Data fetcher: DummyJSON API → CSV → S3
├── requirements.txt           # Python dependencies (local script)
├── glue_job/
│   └── etl_job.py             # Glue ETL job script (copy-paste into AWS Console)
├── sample_data/               # Generated CSV files (created by fetch_data.py)
└── README.md
```

---

# Step-by-Step Setup (AWS Console)

## Step 1: Create an AWS Account

1. Go to https://aws.amazon.com/ and click **"Create an AWS Account"**
2. Follow the sign-up wizard (email, password, payment method)
3. Select the **Free Tier** plan
4. Once done, sign in to the **AWS Management Console** at https://console.aws.amazon.com/

---

## Step 2: Create the S3 Bucket

We need one bucket with two prefixes — one for raw CSV input, one for processed Parquet output.

1. Go to **AWS Console** → search **"S3"** → click **S3**
2. Click **"Create bucket"**
3. Fill in:
   - **Bucket name**: `ecommerce-glue-etl-demo` (must be globally unique — add your name, e.g. `ecommerce-glue-etl-demo-john`)
   - **AWS Region**: Choose your nearest region (e.g., `us-west-2`)
4. Leave all other settings as default (Block all public access = ON is fine)
5. Click **"Create bucket"**
6. Open the newly created bucket and create two folders:
   - Click **"Create folder"** → name: `raw-data` → **"Create folder"**
   - Click **"Create folder"** → name: `processed-data` → **"Create folder"**

> Your bucket now has `raw-data/` (for CSV input) and `processed-data/` (for Parquet output)
>
> **Note:** You do NOT need to create a `sales/` subfolder inside `raw-data/`. The `fetch_data.py` script uploads to the key `raw-data/sales/<filename>.csv`, and S3 automatically creates the `sales/` prefix on upload.

---

## Step 3: Create the IAM Role for Glue

AWS Glue needs permission to read/write S3 and manage the Data Catalog.

1. Go to **AWS Console** → search **"IAM"** → click **IAM**
2. In the left sidebar, click **"Roles"**
3. Click **"Create role"**
4. Select:
   - **Trusted entity type**: AWS service
   - **Use case**: Glue
   - Click **"Next"**
5. Add these permission policies (search and check each one):
   - `AmazonS3FullAccess`
   - `AWSGlueServiceRole`
   - `CloudWatchLogsFullAccess`
6. Click **"Next"**
7. Fill in:
   - **Role name**: `ecommerce-glue-etl-role`
   - **Description**: "Role for E-Commerce Glue ETL pipeline"
8. Click **"Create role"**

> Remember this role name — you'll select it when creating the Crawler and ETL Job

---

## Step 4: Create the Glue Database

1. Go to **AWS Console** → search **"AWS Glue"** → click **AWS Glue**
2. In the left sidebar, click **"Databases"** (under Data Catalog)
3. Click **"Add database"**
4. Fill in:
   - **Database name**: `ecommerce_sales_db`
5. Click **"Create database"**

> This database will hold the table(s) that the Crawler discovers

---

## Step 5: Create the Glue Crawler

The Crawler scans your CSV files in S3 and registers them as tables in the Data Catalog.

1. In **AWS Glue**, click **"Crawlers"** in the left sidebar
2. Click **"Create crawler"**
3. **Name**: `ecommerce-sales-crawler`
4. Click **"Next"**
5. **Data source configuration**:
   - Click **"Add a data source"**
   - **Data source**: S3
   - **S3 path**: `s3://<your-bucket>/raw-data/sales/` (browse and select)
   - Click **"Add an S3 data source"**
6. Click **"Next"**
7. **IAM role**: Select **"Choose an existing IAM role"**
   - Choose: `ecommerce-glue-etl-role`
8. Click **"Next"**
9. **Target database**: Select `ecommerce_sales_db`
10. **Table name prefix** (optional): leave empty
11. Click **"Next"** → Review → **"Create crawler"**

### Run the Crawler

> **Don't run the crawler yet!** — we'll upload data first in Step 7, then run the crawler.

---

## Step 6: Create the Glue ETL Job

1. In **AWS Glue**, click **"ETL jobs"** in the left sidebar
2. Click **"Script editor"** (we'll paste our own PySpark script)
3. Select **"Spark"** as the engine → Click **"Create script"**
4. **Delete all the default code** in the editor
5. Open the file `glue_job/etl_job.py` from this project
6. **Copy the entire contents** and paste it into the Glue script editor
7. Click the **"Job details"** tab at the top
8. Fill in:
   - **Name**: `ecommerce-sales-etl`
   - **IAM Role**: Select `ecommerce-glue-etl-role`
   - **Glue version**: Glue 4.0 (recommended)
   - **Language**: Python 3
   - **Worker type**: G.1X (smallest)
   - **Requested number of workers**: 2 (minimum)
   - **Job timeout (minutes)**: 10
9. Scroll down to **"Job parameters"** and add these key-value pairs:
   - `--DATABASE_NAME` → `ecommerce_sales_db`
   - `--TABLE_NAME` → `sales` (this is the table name the Crawler will create — it comes from the folder name `raw-data/sales/`)
   - `--OUTPUT_BUCKET` → `<your-bucket-name>` (e.g., `ecommerce-glue-etl-demo-john`)
10. Click **"Save"**

> **Note on dependencies:** The ETL script only uses `pyspark` and `awsglue` libraries, which are **pre-installed** in the AWS Glue runtime — no extra packages needed.

---

## Step 7: Fetch Data and Upload to S3

Back on your **local machine**:

```bash
# Install boto3
pip install boto3

# Configure AWS CLI with your credentials
aws configure
# Enter: Access Key ID, Secret Access Key, Region, Output format (json)

# Fetch real product data and generate 5000 orders, upload to S3
python fetch_data.py --bucket <your-bucket-name> --orders 5000 --days 30
```

Replace `<your-bucket-name>` with the bucket you created (e.g., `ecommerce-glue-etl-demo-john`).

This will:
1. Fetch 20 real products from the Fake Store API
2. Fetch 10 users for customer data
3. Generate 5000 realistic orders as CSV
4. Save the CSV locally in `sample_data/`
5. Upload the CSV to `s3://<your-bucket>/raw-data/sales/`

### Test Locally First (Optional)

```bash
# Just save CSV locally, no AWS needed
python fetch_data.py --local-only --orders 1000 --days 7
```

---

## Step 8: Run the Crawler

Now that data is in S3, run the Crawler to catalog it.

1. Go to **AWS Glue** → **Crawlers** → select `ecommerce-sales-crawler`
2. Click **"Run crawler"**
3. Wait until the status shows **"Ready"** (takes ~1-2 minutes)
4. Check the results:
   - Go to **Databases** → `ecommerce_sales_db` → **Tables**
   - You should see a table called `sales`
   - Click on it to see the schema (column names and types inferred from your CSV)

---

## Step 9: Run the Glue ETL Job

1. Go to **AWS Glue** → **ETL jobs** → select `ecommerce-sales-etl`
2. Click **"Run"**
3. Go to the **"Runs"** tab to monitor progress (takes ~3-5 minutes)
4. Click on the run to see **CloudWatch logs** for detailed output:
   ```
   EXTRACT: Read 5000 records
   TRANSFORM: 5000 records after cleaning
     Completed: 4150 | Returned: 420 | Cancelled: 430
   LOAD: Wrote enriched orders to s3://...
   LOAD: Wrote daily summary to s3://...
   LOAD: Wrote category summary to s3://...
   LOAD: Wrote product summary to s3://...
   ETL JOB COMPLETE
   ```

---

## Step 10: Verify the Results

### Check S3 Output

1. Go to **S3** → your bucket → `processed-data/`
2. You should see 4 folders:
   - `enriched-orders/` — partitioned by `category/` and `order_status/`, Parquet files inside
   - `daily-summary/` — partitioned by `category/`, Parquet files
   - `category-summary/` — Parquet files
   - `product-summary/` — Parquet files

### Query with Athena (Optional but Recommended)

1. **Create a Crawler for the output** (or create tables manually):
   - Create a new Crawler pointing to `s3://<your-bucket>/processed-data/enriched-orders/`
   - Target database: `ecommerce_sales_db`
   - Run it to register the Parquet data as a table

2. Go to **AWS Console** → search **"Athena"** → click **Athena**
3. Set up a query result location (first time only):
   - Click **"Settings"** → **"Manage"**
   - Set S3 path: `s3://<your-bucket>/athena-results/`
   - Click **"Save"**
4. Select database: `ecommerce_sales_db`
5. Try these queries:

```sql
-- Top 5 products by revenue
SELECT product_name, category, price_tier, rating_tier,
       COUNT(*) as orders, SUM(total_amount) as revenue
FROM enriched_orders
WHERE order_status = 'completed'
GROUP BY product_name, category, price_tier, rating_tier
ORDER BY revenue DESC
LIMIT 5;

-- Revenue by day of week
SELECT day_name, COUNT(*) as orders, 
       ROUND(SUM(total_amount), 2) as revenue
FROM enriched_orders
WHERE order_status = 'completed'
GROUP BY day_name
ORDER BY revenue DESC;

-- Category return rates
SELECT category,
       COUNT(*) as total_orders,
       SUM(CASE WHEN order_status = 'returned' THEN 1 ELSE 0 END) as returns,
       ROUND(100.0 * SUM(CASE WHEN order_status = 'returned' THEN 1 ELSE 0 END) / COUNT(*), 1) as return_rate_pct
FROM enriched_orders
GROUP BY category
ORDER BY return_rate_pct DESC;
```

---

## Cleanup (After Demo)

To avoid any charges, delete everything:

1. **S3**: Open your bucket → select all files → **Delete** → then **Delete bucket**
2. **Glue ETL Job**: Go to Glue → ETL jobs → select `ecommerce-sales-etl` → **Actions** → **Delete**
3. **Glue Crawler**: Go to Glue → Crawlers → select `ecommerce-sales-crawler` → **Actions** → **Delete**
4. **Glue Database**: Go to Glue → Databases → select `ecommerce_sales_db` → **Delete**
5. **IAM Role**: Go to IAM → Roles → select `ecommerce-glue-etl-role` → **Delete**

## Cost

With AWS Free Tier, this demo should cost **under $1**:
- **S3**: 5 GB free storage, negligible for CSV/Parquet files
- **Glue Crawler**: First 1M objects free/month
- **Glue ETL Job**: Billed per DPU-hour (~$0.44/DPU-hour, job runs ~3 min with 2 DPUs ≈ $0.04)
- **Athena**: $5 per TB scanned (Parquet is compressed — a few MB = fraction of a cent)

> **Tip:** Glue is not part of the always-free tier like Lambda/DynamoDB, but the cost for this demo is negligible. Delete resources after the demo to avoid any ongoing charges.
