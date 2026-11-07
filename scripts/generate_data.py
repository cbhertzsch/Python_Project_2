
from faker import Faker
import csv
import json
import random
from pathlib import Path

fake = Faker()
random.seed(42)
Faker.seed(42)
RECORD_COUNT = 100000

data_folder = Path("/data/raw")
data_folder.mkdir(parents=True, exist_ok=True)

customers = []

for i in range(1, RECORD_COUNT + 1):
    customer = {
        "customer_id": i,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": f"customer{i}@example.com",
        "phone": fake.numerify("###-###-####"),
        "city": fake.city(),
        "registration_date": str(fake.date_between(start_date="-3y", end_date="today")),
        "customer_type": random.choice(["Regular", "Premium", "VIP"])
    }
    customers.append(customer)

# Add exact duplicate customer records
for i in range(10):
    customers.append(customers[i].copy())

# Add near-duplicates with the same customer ID
for i in range(10, 20):
    duplicate = customers[i].copy()
    duplicate["city"] = duplicate["city"].upper()
    customers.append(duplicate)
# Add missing email addresses to some customers
for i in range(20, 30):
    customers[i]["email"] = None
# Add inconsistent city capitalization
for i in range(30, 40):
    customers[i]["city"] = "new york"

for i in range(40, 50):
    customers[i]["city"] = "NEW YORK"

for i in range(50, 60):
    customers[i]["city"] = "New York"
# Add extra whitespace to customer names and cities
for i in range(60, 70):
    customers[i]["first_name"] = "  " + customers[i]["first_name"] + "  "

for i in range(70, 80):
    customers[i]["city"] = "  " + customers[i]["city"] + "  "

# Add inconsistent registration date formats
from datetime import datetime

for i in range(80, 90):
    date = datetime.strptime(customers[i]["registration_date"], "%Y-%m-%d")
    customers[i]["registration_date"] = date.strftime("%m/%d/%Y")

for i in range(90, 100):
    date = datetime.strptime(customers[i]["registration_date"], "%Y-%m-%d")
    customers[i]["registration_date"] = date.strftime("%b %d, %Y")

print("Generated", len(customers), "customers")


products = []

categories = ["Electronics", "Clothing", "Home", "Sports", "Books"]
brands = ["Brand A", "Brand B", "Brand C", "Brand D"]

for i in range(1, RECORD_COUNT + 1):
    product = {
        "product_id": i,
        "product_name": fake.word().title() + " Product",
        "category": random.choice(categories),
        "brand": random.choice(brands),
        "unit_price": round(random.uniform(5, 500), 2),
        "stock_quantity": random.randint(0, 500),
        "is_active": random.choice([True, False])
    }
    products.append(product)
# Add missing product names
for i in range(20, 30):
    products[i]["product_name"] = None
# Add exact duplicate product records
for i in range(10):
    products.append(products[i].copy())

# Add near-duplicate products with the same product ID
for i in range(10, 20):
    duplicate = products[i].copy()
    duplicate["product_name"] = duplicate["product_name"].upper()
    products.append(duplicate)
# Add invalid negative product prices
for i in range(30, 35):
    products[i]["unit_price"] = -10.00

print("Generated", len(products), "products")


orders = []

for i in range(1, RECORD_COUNT + 1):
    order = {
        "order_id": i,
        "customer_id": random.choice(customers)["customer_id"],
	"product_id": random.randint(1, RECORD_COUNT),
        "order_date": str(fake.date_time_between(start_date="-1y", end_date="now")),
        "order_status": random.choice(["Pending", "Shipped", "Delivered"]),
        "payment_method": random.choice(["Credit Card", "Debit Card", "PayPal"]),
        "total_amount": round(random.uniform(20, 1000), 2),
        "discount_amount": round(random.uniform(0, 20), 2),
        "shipping_cost": round(random.uniform(0, 25), 2)
    }
    orders.append(order)

# Add invalid negative order amounts
for i in range(20, 25):
    orders[i]["total_amount"] = -50.00

# Add discounts greater than the order total
for i in range(25, 30):
    orders[i]["total_amount"] = 100.00
    orders[i]["discount_amount"] = 150.00

# Add orphan orders with customer IDs that do not exist
for i in range(30, 40):
    orders[i]["customer_id"] = RECORD_COUNT + 1000 + i

# Add exact duplicate order records
for i in range(10):
    orders.append(orders[i].copy())

# Add near-duplicate orders with the same order ID
for i in range(10, 20):
    duplicate = orders[i].copy()
    duplicate["order_status"] = duplicate["order_status"].upper()
    orders.append(duplicate)

# Add missing order amounts
for i in range(40, 50):
    orders[i]["total_amount"] = None

# Add inconsistent payment method capitalization
for i in range(50, 60):
    orders[i]["payment_method"] = orders[i]["payment_method"].lower()

# Add extra whitespace to order statuses
for i in range(60, 70):
    orders[i]["order_status"] = "  " + orders[i]["order_status"] + "  "

print("Generated", len(orders), "orders")


import pandas as pd
import yaml

datasets = {
    "customers": customers,
    "products": products,
    "orders": orders
}

for name, records in datasets.items():

    # Save as CSV
    df = pd.DataFrame(records)
    df.to_csv(data_folder / f"{name}.csv", index=False)

    # Save as JSON
    df.to_json(data_folder / f"{name}.json", orient="records", indent=4)

    # Save as YAML
    with open(data_folder / f"{name}.yaml", "w") as file:
        yaml.safe_dump(records, file, sort_keys=False)

    print("Saved", name, "in CSV, JSON, and YAML")
