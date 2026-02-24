#!/usr/bin/env python3
"""
Data Generator Script for Sample CSV Files

Generates customers.csv, products.csv, orders.csv, and order_details.csv
with proper foreign key relationships.

Usage:
    python scripts/generate_sample_data.py [customer_count] [product_count] [order_count] [max_items_per_order_count]

Arguments:
    customer_count: Number of customers to generate (default: 10)
    product_count: Number of products to generate (default: 10)
    order_count: Number of orders to generate (default: 10)
    max_items_per_order_count: Maximum items per order (default: 10)
"""

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict


# Sample data for generation
FIRST_NAMES = [
    "John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Edward", "Fiona",
    "George", "Helen", "Ivan", "Julia", "Kevin", "Laura", "Michael", "Nancy",
    "Oliver", "Patricia", "Quinn", "Rachel", "Samuel", "Teresa", "Ulysses",
    "Victoria", "Walter", "Xena", "Yolanda", "Zachary"
]

LAST_NAMES = [
    "Doe", "Smith", "Johnson", "Williams", "Brown", "Davis", "Miller", "Wilson",
    "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin",
    "Thompson", "Garcia", "Martinez", "Robinson", "Clark", "Rodriguez", "Lewis", "Lee"
]

CITIES_STATES = [
    ("New York", "NY", "10001"),
    ("Los Angeles", "CA", "90001"),
    ("Chicago", "IL", "60601"),
    ("Houston", "TX", "77001"),
    ("Phoenix", "AZ", "85001"),
    ("Philadelphia", "PA", "19101"),
    ("San Antonio", "TX", "78201"),
    ("San Diego", "CA", "92101"),
    ("Dallas", "TX", "75201"),
    ("San Jose", "CA", "95101"),
    ("Austin", "TX", "78701"),
    ("Jacksonville", "FL", "32099"),
    ("Fort Worth", "TX", "76101"),
    ("Columbus", "OH", "43004"),
    ("Charlotte", "NC", "28201"),
    ("San Francisco", "CA", "94102"),
    ("Indianapolis", "IN", "46201"),
    ("Seattle", "WA", "98101"),
    ("Denver", "CO", "80201"),
    ("Boston", "MA", "02101")
]

STREET_NAMES = [
    "Main St", "Oak Ave", "Pine Rd", "Elm St", "Maple Dr", "Cedar Ln",
    "Birch Ct", "Spruce Way", "Ash Blvd", "Walnut Pl", "Cherry St",
    "Willow Ave", "Poplar Rd", "Hickory Dr", "Sycamore Ln"
]

PRODUCT_CATEGORIES = {
    "Electronics": [
        ("Laptop Pro 15", "High-performance laptop with 15-inch display", 1299.99),
        ("Wireless Mouse", "Ergonomic wireless mouse with USB receiver", 29.99),
        ("USB-C Cable", "6ft USB-C charging cable", 15.99),
        ("Mechanical Keyboard", "RGB mechanical gaming keyboard", 149.99),
        ("Monitor 27inch", "4K UHD 27-inch monitor", 399.99),
        ("Webcam HD", "1080p HD webcam with microphone", 79.99),
        ("Headphones", "Noise-cancelling wireless headphones", 199.99),
        ("Printer", "All-in-one wireless printer", 179.99),
        ("Tablet", "10-inch tablet with stylus", 449.99),
        ("Smart Speaker", "Voice-controlled smart speaker", 89.99),
        ("External SSD", "1TB portable SSD drive", 129.99),
        ("Wireless Charger", "Fast wireless charging pad", 34.99),
        ("Gaming Console", "Next-gen gaming console", 499.99),
        ("Bluetooth Earbuds", "True wireless earbuds", 149.99),
        ("Smart Watch", "Fitness tracking smartwatch", 299.99)
    ],
    "Furniture": [
        ("Office Chair", "Ergonomic office chair with lumbar support", 249.99),
        ("Standing Desk", "Adjustable height standing desk", 499.99),
        ("Desk Lamp", "LED desk lamp with adjustable brightness", 45.99),
        ("Bookshelf", "5-tier wooden bookshelf", 159.99),
        ("Filing Cabinet", "3-drawer metal filing cabinet", 189.99),
        ("Conference Table", "8-person conference table", 799.99),
        ("Lounge Chair", "Comfortable reading chair", 349.99),
        ("Side Table", "Modern side table with drawer", 89.99)
    ],
    "Stationery": [
        ("Notebook Set", "Pack of 5 ruled notebooks", 12.99),
        ("Pen Set", "Premium ballpoint pen set", 24.99),
        ("Sticky Notes", "Assorted color sticky notes pack", 8.99),
        ("Desk Organizer", "Multi-compartment desk organizer", 19.99),
        ("Whiteboard", "Magnetic dry-erase whiteboard", 49.99),
        ("Paper Clips", "Box of 1000 paper clips", 5.99),
        ("Stapler", "Heavy-duty stapler", 15.99)
    ],
    "Accessories": [
        ("Backpack", "Laptop backpack with multiple compartments", 59.99),
        ("Water Bottle", "Insulated stainless steel water bottle", 24.99),
        ("Phone Stand", "Adjustable phone and tablet stand", 19.99),
        ("Cable Organizer", "Cable management box", 16.99),
        ("Laptop Sleeve", "Protective laptop sleeve 15-inch", 29.99),
        ("Mouse Pad", "Extended gaming mouse pad", 22.99),
        ("USB Hub", "7-port USB 3.0 hub", 39.99)
    ]
}

SUPPLIERS = ["TechCorp", "CableMax", "ComfortSeating", "DeskPro", "PaperPlus",
             "DisplayTech", "LightWorks", "BagMaster", "HydroGear", "AudioMax",
             "StandPro", "PrintTech", "FurniturePlus", "OfficeSupply"]

ORDER_STATUSES = ["completed", "shipped", "processing", "cancelled"]
PAYMENT_METHODS = ["credit_card", "paypal", "debit_card"]

ORDER_NOTES = [
    "Express delivery",
    "Gift wrap requested",
    "Fragile items",
    "Customer requested cancellation",
    "Valentine's Day order",
    "Birthday gift",
    "Leave at door",
    "Call before delivery",
    ""
]


def generate_customers(count: int, start_date: datetime) -> List[Dict]:
    """Generate customer data."""
    customers = []
    for i in range(1, count + 1):
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        city, state, zip_code = random.choice(CITIES_STATES)
        street_num = random.randint(100, 999)
        street = random.choice(STREET_NAMES)
        
        customer = {
            "customer_id": i,
            "first_name": first_name,
            "last_name": last_name,
            "email": f"{first_name.lower()}.{last_name.lower()}@email.com",
            "phone": f"555-{random.randint(1000, 9999)}",
            "address": f"{street_num} {street}",
            "city": city,
            "state": state,
            "zip_code": zip_code,
            "country": "USA",
            "created_date": (start_date + timedelta(days=i-1)).strftime("%Y-%m-%d")
        }
        customers.append(customer)
    
    return customers


def generate_products(count: int, start_date: datetime) -> List[Dict]:
    """Generate product data."""
    products = []
    product_id = 101
    
    # Flatten all products from categories
    all_products = []
    for category, items in PRODUCT_CATEGORIES.items():
        for name, desc, price in items:
            all_products.append((category, name, desc, price))
    
    # Generate products
    for i in range(count):
        if i < len(all_products):
            category, name, desc, price = all_products[i]
        else:
            # Generate random products if we need more
            category = random.choice(list(PRODUCT_CATEGORIES.keys()))
            name = f"Product {i+1}"
            desc = f"Description for product {i+1}"
            price = round(random.uniform(9.99, 999.99), 2)
        
        # Generate SKU
        sku = f"{name[:3].upper()}-{random.randint(100, 999)}-{random.randint(10, 99)}"
        
        product = {
            "product_id": product_id,
            "product_name": name,
            "category": category,
            "description": desc,
            "price": price,
            "stock_quantity": random.randint(10, 500),
            "supplier": random.choice(SUPPLIERS),
            "sku": sku,
            "created_date": (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        }
        products.append(product)
        product_id += 1
    
    return products


def generate_orders(count: int, customer_ids: List[int], start_date: datetime) -> List[Dict]:
    """Generate order data."""
    orders = []
    order_id = 1001
    
    for i in range(count):
        customer_id = random.choice(customer_ids)
        status = random.choice(ORDER_STATUSES)
        payment_method = random.choice(PAYMENT_METHODS)
        
        # Generate shipping address (simplified)
        city, state, zip_code = random.choice(CITIES_STATES)
        street_num = random.randint(100, 999)
        street = random.choice(STREET_NAMES)
        shipping_address = f"{street_num} {street} {city} {state} {zip_code}"
        
        order = {
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": (start_date + timedelta(days=i)).strftime("%Y-%m-%d"),
            "status": status,
            "payment_method": payment_method,
            "shipping_address": shipping_address,
            "notes": random.choice(ORDER_NOTES)
        }
        orders.append(order)
        order_id += 1
    
    return orders


def generate_order_details(orders: List[Dict], product_ids: List[int], 
                          max_items_per_order: int, products: List[Dict]) -> List[Dict]:
    """Generate order details data."""
    order_details = []
    
    # Create a product lookup for prices
    product_prices = {p["product_id"]: p["price"] for p in products}
    
    for order in orders:
        # Random number of items per order (1 to max_items_per_order)
        num_items = random.randint(1, min(max_items_per_order, len(product_ids)))
        
        # Select random products for this order (no duplicates)
        selected_products = random.sample(product_ids, num_items)
        
        for product_id in selected_products:
            quantity = random.randint(1, 5)
            price = product_prices[product_id]
            amount = round(price * quantity, 2)
            
            detail = {
                "order_id": order["order_id"],
                "product_id": product_id,
                "quantity": quantity,
                "amount": amount
            }
            order_details.append(detail)
    
    return order_details


def write_csv(filepath: Path, data: List[Dict], fieldnames: List[str]):
    """Write data to CSV file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"✓ Generated {filepath} with {len(data)} rows")


def main():
    parser = argparse.ArgumentParser(
        description="Generate sample CSV data files with proper relationships"
    )
    parser.add_argument(
        "customer_count",
        type=int,
        nargs="?",
        default=10,
        help="Number of customers to generate (default: 10)"
    )
    parser.add_argument(
        "product_count",
        type=int,
        nargs="?",
        default=10,
        help="Number of products to generate (default: 10)"
    )
    parser.add_argument(
        "order_count",
        type=int,
        nargs="?",
        default=10,
        help="Number of orders to generate (default: 10)"
    )
    parser.add_argument(
        "max_items_per_order_count",
        type=int,
        nargs="?",
        default=10,
        help="Maximum items per order (default: 10)"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.customer_count < 1:
        print("Error: customer_count must be at least 1")
        return
    if args.product_count < 1:
        print("Error: product_count must be at least 1")
        return
    if args.order_count < 1:
        print("Error: order_count must be at least 1")
        return
    if args.max_items_per_order_count < 1:
        print("Error: max_items_per_order_count must be at least 1")
        return
    
    print(f"\nGenerating sample data:")
    print(f"  Customers: {args.customer_count}")
    print(f"  Products: {args.product_count}")
    print(f"  Orders: {args.order_count}")
    print(f"  Max items per order: {args.max_items_per_order_count}\n")
    
    # Set base dates
    customer_start_date = datetime(2024, 1, 15)
    product_start_date = datetime(2024, 1, 1)
    order_start_date = datetime(2024, 2, 1)
    
    # Generate data
    customers = generate_customers(args.customer_count, customer_start_date)
    products = generate_products(args.product_count, product_start_date)
    
    customer_ids = [c["customer_id"] for c in customers]
    product_ids = [p["product_id"] for p in products]
    
    orders = generate_orders(args.order_count, customer_ids, order_start_date)
    order_details = generate_order_details(orders, product_ids, 
                                          args.max_items_per_order_count, products)
    
    # Define output directory
    output_dir = Path("data/sample")
    
    # Write CSV files
    write_csv(
        output_dir / "customers.csv",
        customers,
        ["customer_id", "first_name", "last_name", "email", "phone", "address",
         "city", "state", "zip_code", "country", "created_date"]
    )
    
    write_csv(
        output_dir / "products.csv",
        products,
        ["product_id", "product_name", "category", "description", "price",
         "stock_quantity", "supplier", "sku", "created_date"]
    )
    
    write_csv(
        output_dir / "orders.csv",
        orders,
        ["order_id", "customer_id", "order_date", "status", "payment_method",
         "shipping_address", "notes"]
    )
    
    write_csv(
        output_dir / "order_details.csv",
        order_details,
        ["order_id", "product_id", "quantity", "amount"]
    )
    
    print(f"\n✓ All files generated successfully in {output_dir}/")
    print(f"\nSummary:")
    print(f"  - {len(customers)} customers")
    print(f"  - {len(products)} products")
    print(f"  - {len(orders)} orders")
    print(f"  - {len(order_details)} order detail records")


if __name__ == "__main__":
    main()
