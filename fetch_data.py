"""
Data Fetcher Script
Fetches real product and user data from the DummyJSON API (free, no API key required),
generates realistic e-commerce orders around those products, saves as CSV,
and uploads to S3 to be processed by the AWS Glue ETL pipeline.

Data Source: https://dummyjson.com — free, open, no authentication needed.

Usage:
    python fetch_data.py --bucket <s3-bucket-name> [--orders 5000] [--local-only]
"""

import argparse
import csv
import io
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen, Request

try:
    import boto3
except ImportError:
    boto3 = None

# Seed for reproducibility (students get the same data if they use the same seed)
random.seed(42)

DUMMYJSON_API = "https://dummyjson.com"

REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", "ap-northeast-1"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "gift_card", "buy_now_pay_later"]
ORDER_STATUSES = ["completed", "completed", "completed", "completed", "returned", "cancelled"]

# Category-level return rates (higher for fashion/fragrance, lower for groceries)
CATEGORY_RETURN_RATES = {
    "smartphones": 0.08,
    "laptops": 0.06,
    "fragrances": 0.12,
    "skincare": 0.10,
    "groceries": 0.02,
    "home-decoration": 0.07,
    "furniture": 0.09,
    "tops": 0.14,
    "womens-dresses": 0.15,
    "womens-shoes": 0.13,
    "mens-shirts": 0.12,
    "mens-shoes": 0.11,
    "mens-watches": 0.06,
    "womens-watches": 0.06,
    "womens-bags": 0.08,
    "womens-jewellery": 0.07,
    "sunglasses": 0.05,
    "automotive": 0.04,
    "motorcycle": 0.03,
    "lighting": 0.06,
    "beauty": 0.10,
}


def fetch_products() -> list[dict]:
    """Fetch products from the DummyJSON API."""
    url = f"{DUMMYJSON_API}/products?limit=50"
    print(f"  Fetching: {url}")
    req = Request(url, headers={"User-Agent": "AWS-Glue-Pipeline-Demo/1.0"})
    with urlopen(req, timeout=30) as response:
        data = json.loads(response.read().decode())
    products = data.get("products", [])
    categories = set(p["category"] for p in products)
    print(f"  Fetched {len(products)} products across {len(categories)} categories")
    return products


def fetch_users() -> list[dict]:
    """Fetch users from the DummyJSON API for realistic customer data."""
    url = f"{DUMMYJSON_API}/users?limit=30&select=id,firstName,lastName,address"
    print(f"  Fetching: {url}")
    req = Request(url, headers={"User-Agent": "AWS-Glue-Pipeline-Demo/1.0"})
    with urlopen(req, timeout=30) as response:
        data = json.loads(response.read().decode())
    users = data.get("users", [])
    print(f"  Fetched {len(users)} users")
    return users


def generate_orders(products: list[dict], users: list[dict], num_orders: int, days: int) -> list[dict]:
    """Generate realistic e-commerce order data using real products and users."""
    orders = []
    now = datetime.now(timezone.utc)
    start_date = now - timedelta(days=days)

    # Build customer info from real API users
    customer_ids = [f"CUST-{u['id']:04d}" for u in users]
    customer_names = {f"CUST-{u['id']:04d}": f"{u['firstName']} {u['lastName']}" for u in users}
    customer_cities = {
        f"CUST-{u['id']:04d}": u.get("address", {}).get("city", "Unknown")
        for u in users
    }

    for _ in range(num_orders):
        # Pick a random product (from the real API data)
        product = random.choice(products)
        product_name = product["title"]
        category = product["category"]
        base_price = product["price"]
        rating = product.get("rating", 0)
        brand = product.get("brand", "Unknown")

        # Slight price variation (+/- 15%) to simulate market fluctuation
        unit_price = round(base_price * random.uniform(0.85, 1.15), 2)

        # Generate order details
        quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
        subtotal = round(unit_price * quantity, 2)

        # Discount (30% of orders get a discount)
        discount_pct = random.choice([0, 0, 0, 0, 0, 0, 0, 5, 10, 15, 20, 25])
        discount_amount = round(subtotal * discount_pct / 100, 2)
        total = round(subtotal - discount_amount, 2)

        # Status — use category return rate to influence returns
        return_rate = CATEGORY_RETURN_RATES.get(category, 0.05)
        status = random.choice(ORDER_STATUSES)
        if status == "returned" and random.random() > return_rate * 5:
            status = "completed"

        # Customer
        customer_id = random.choice(customer_ids)

        # Timestamps
        order_time = start_date + timedelta(
            seconds=random.randint(0, int((now - start_date).total_seconds()))
        )

        order = {
            "order_id": str(uuid.uuid4()),
            "customer_id": customer_id,
            "customer_name": customer_names.get(customer_id, "Unknown"),
            "customer_city": customer_cities.get(customer_id, "Unknown"),
            "order_date": order_time.strftime("%Y-%m-%d"),
            "order_timestamp": order_time.isoformat(),
            "product_id": product["id"],
            "product_name": product_name,
            "brand": brand,
            "category": category,
            "unit_price": unit_price,
            "quantity": quantity,
            "subtotal": subtotal,
            "discount_pct": discount_pct,
            "discount_amount": discount_amount,
            "total_amount": total,
            "payment_method": random.choice(PAYMENT_METHODS),
            "region": random.choice(REGIONS),
            "order_status": status,
            "product_rating": rating,
        }
        orders.append(order)

    # Sort by timestamp
    orders.sort(key=lambda x: x["order_timestamp"])
    return orders


def rows_to_csv_string(rows: list[dict]) -> str:
    """Convert list of dicts to CSV string."""
    if not rows:
        return ""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def save_local(csv_string: str, filename: str) -> str:
    """Save CSV to local file."""
    os.makedirs("sample_data", exist_ok=True)
    filepath = os.path.join("sample_data", filename)
    with open(filepath, "w") as f:
        f.write(csv_string)
    print(f"Saved locally: {filepath}")
    return filepath


def upload_to_s3(csv_string: str, bucket: str, key: str):
    """Upload CSV string to S3."""
    if boto3 is None:
        raise RuntimeError("boto3 is required for S3 upload. Install it with: pip install boto3")
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_string.encode("utf-8"),
        ContentType="text/csv",
    )
    print(f"Uploaded to S3: s3://{bucket}/{key}")


def main():
    parser = argparse.ArgumentParser(description="Generate e-commerce sales data and upload to S3")
    parser.add_argument("--bucket", type=str, help="S3 bucket name")
    parser.add_argument("--orders", type=int, default=5000, help="Number of orders to generate (default: 5000)")
    parser.add_argument("--days", type=int, default=30, help="Span of days for order dates (default: 30)")
    parser.add_argument("--local-only", action="store_true", help="Save locally only, skip S3 upload")
    args = parser.parse_args()

    if not args.local_only and not args.bucket:
        parser.error("--bucket is required unless --local-only is specified")

    # Fetch real data from the DummyJSON API
    print("Fetching real product and user data from DummyJSON API...\n")
    products = fetch_products()
    users = fetch_users()

    print(f"Generating {args.orders} orders spanning {args.days} days...\n")
    orders = generate_orders(products, users, args.orders, args.days)
    print(f"Total orders generated: {len(orders)}")

    # Summary stats
    completed = sum(1 for o in orders if o["order_status"] == "completed")
    returned = sum(1 for o in orders if o["order_status"] == "returned")
    cancelled = sum(1 for o in orders if o["order_status"] == "cancelled")
    revenue = sum(o["total_amount"] for o in orders if o["order_status"] == "completed")
    print(f"  Completed: {completed} | Returned: {returned} | Cancelled: {cancelled}")
    print(f"  Total revenue (completed): ${revenue:,.2f}\n")

    csv_string = rows_to_csv_string(orders)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"sales_data_{timestamp}.csv"

    # Always save locally
    save_local(csv_string, filename)

    # Upload to S3 if not local-only
    if not args.local_only:
        s3_key = f"raw-data/sales/{filename}"
        upload_to_s3(csv_string, args.bucket, s3_key)
        print(f"\nData uploaded! Run the Glue Crawler to catalog the data, then run the Glue ETL Job.")

    print("Done!")


if __name__ == "__main__":
    main()
