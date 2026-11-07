
from faker import Faker
import csv
import json
import random
from pathlib import Path

fake = Faker()
random.seed(42)
Faker.seed(42)
RECORD_COUNT = 1000

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

print("Generated", len(products), "products")


orders = []

for i in range(1, RECORD_COUNT + 1):
    order = {
        "order_id": i,
        "customer_id": random.choice(customers)["customer_id"],
        "order_date": str(fake.date_time_between(start_date="-1y", end_date="now")),
        "order_status": random.choice(["Pending", "Shipped", "Delivered"]),
        "payment_method": random.choice(["Credit Card", "Debit Card", "PayPal"]),
        "total_amount": round(random.uniform(20, 1000), 2),
        "discount_amount": round(random.uniform(0, 20), 2),
        "shipping_cost": round(random.uniform(0, 25), 2)
    }
    orders.append(order)

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
