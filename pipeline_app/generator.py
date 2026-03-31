import random
import uuid
from datetime import datetime, timedelta
import json
from typing import Any
from faker import Faker
fake = Faker()

def generate_customers(count: int = 100) -> list[dict[str, Any]]:
    return [
        {
            "customer_id": str(uuid.uuid4()),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "phone": fake.phone_number(),
            "address": fake.street_address(),
            "city": fake.city(),
            "country": fake.country(),
            "registration_date": fake.date_time_between(start_date="-2y", end_date="now"),
        }
        for _ in range(count)
    ]

def generate_products(count: int = 50) -> list[dict[str, Any]]:
    categories = ["Electronics", "Clothing", "Home", "Books", "Toys", "Sports", "Beauty"]
    products = []
    for _ in range(count):
        price = round(random.uniform(5, 500), 2)
        products.append(
            {
                "product_id": str(uuid.uuid4()),
                "name": f"{fake.word().capitalize()} {fake.word()}",
                "category": random.choice(categories),
                "price": price,
                "cost": round(price * random.uniform(0.3, 0.7), 2),
                "stock_quantity": random.randint(0, 1000),
                "created_at": fake.date_time_between(start_date="-2y", end_date="-1y"),
            }
        )
    return products

def generate_orders(
    customers: list[dict[str, Any]], products: list[dict[str, Any]], count: int = 1000
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    orders: list[dict[str, Any]] = []
    order_items: list[dict[str, Any]] = []
    statuses = ["completed", "processing", "shipped", "delivered", "cancelled"]
    customer_ids = [customer["customer_id"] for customer in customers]

    for _ in range(count):
        customer_id = random.choice(customer_ids)
        customer = next(item for item in customers if item["customer_id"] == customer_id)
        order_date = fake.date_time_between(start_date="-1y", end_date="now")
        order_id = str(uuid.uuid4())
        items = random.sample(products, random.randint(1, min(5, len(products))))
        order_total = 0.0

        for product in items:
            quantity = random.randint(1, 5)
            unit_price = float(product["price"])
            item_total = round(quantity * unit_price, 2)
            order_total += item_total
            order_items.append(
                {
                    "order_item_id": str(uuid.uuid4()),
                    "order_id": order_id,
                    "product_id": product["product_id"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "total_price": item_total,
                }
            )

        orders.append(
            {
                "order_id": str(order_id),
                "customer_id": customer_id,
                "order_date": order_date,
                "total_amount": round(order_total, 2),
                "status": random.choice(statuses),
                "shipping_address": customer["address"],
            }
        )

    return orders, order_items

def generate_payments(orders: list[dict[str, Any]], count: int = 1000) -> list[dict[str, Any]]:
    payment_methods = ["credit_card", "paypal", "apple_pay", "google_pay"]
    statuses = ["completed", "pending", "failed", "refunded"]
    payments = []

    for _ in range(count):
        order = random.choice(orders)
        base_order_time = order["order_date"]
        if isinstance(base_order_time, str):
            base_order_time = datetime.fromisoformat(base_order_time)
        created_at = base_order_time + timedelta(minutes=random.randint(0, 60))
        payments.append(
            {
                "payment_id": str(uuid.uuid4()),
                "order_id": order["order_id"],
                "amount": order["total_amount"],
                "payment_method": random.choice(payment_methods),
                "status": random.choices(statuses, weights=[0.8, 0.1, 0.05, 0.05])[0],
                "transaction_id": f"txn_{random.randint(1000000000, 9999999999)}",
                "created_at": created_at,
                "updated_at": created_at,
            }
        )

    return payments

def generate_incidents(count: int = 20) -> list[dict[str, Any]]:
    services = ["checkout", "search", "catalog", "payment", "auth"]
    severities = ["critical", "high", "medium", "low"]
    statuses = ["open", "investigating", "resolved", "closed"]
    incidents = []

    for _ in range(count):
        start_time = fake.date_time_between(start_date="-30d", end_date="now")
        end_time = start_time + timedelta(minutes=random.randint(5, 240))
        incidents.append(
            {
                "incident_id": str(uuid.uuid4()),
                "title": f"Service disruption in {random.choice(services).capitalize()}",
                "description": fake.paragraph(),
                "service": random.choice(services),
                "severity": random.choices(severities, weights=[0.1, 0.2, 0.3, 0.4])[0],
                "status": random.choice(statuses),
                "start_time": start_time,
                "end_time": end_time,
                "resolution_time_minutes": int((end_time - start_time).total_seconds() / 60),
                "impact_description": fake.sentence(),
                "created_at": start_time,
                "updated_at": end_time,
            }
        )

    return incidents

def generate_support_tickets(
    customers: list[dict[str, Any]], orders: list[dict[str, Any]], count: int = 200
) -> list[dict[str, Any]]:
    subjects = [
        "Order not received",
        "Defective product",
        "Wrong item shipped",
        "Billing issue",
        "Return request",
        "Refund status",
        "Product question",
    ]
    priorities = ["low", "medium", "high", "urgent"]
    statuses = ["open", "investigating", "resolved", "closed"]
    tickets = []

    for _ in range(count):
        customer = random.choice(customers)
        order = random.choice(orders) if orders and random.random() > 0.3 else None
        created_at = fake.date_time_between(start_date="-30d", end_date="now")
        resolved = random.random() > 0.3
        resolved_at = created_at + timedelta(hours=random.randint(1, 72)) if resolved else None
        tickets.append(
            {
                "ticket_id": str(uuid.uuid4()),
                "customer_id": customer["customer_id"],
                "order_id": order["order_id"] if order else None,
                "subject": random.choice(subjects),
                "description": fake.paragraph(),
                "status": random.choice(statuses),
                "priority": random.choices(priorities, weights=[0.4, 0.3, 0.2, 0.1])[0],
                "assigned_to": fake.name(),
                "created_at": created_at,
                "resolved_at": resolved_at,
                "resolution_time_minutes": int((resolved_at - created_at).total_seconds() / 60)
                if resolved_at
                else None,
            }
        )

    return tickets

def generate_marketing_spend(count: int = 50) -> list[dict[str, Any]]:
    channels = ["google_ads", "facebook_ads", "instagram_ads", "email", "affiliate", "display"]
    campaigns = []

    for _ in range(count):
        spend_date = fake.date_between(start_date="-90d", end_date="today")
        campaigns.append(
            {
                "spend_id": str(uuid.uuid4()),
                "campaign_name": f"{fake.word().capitalize()}_{fake.word()}_{spend_date.strftime('%Y%m')}",
                "channel": random.choice(channels),
                "spend_date": spend_date,
                "amount": round(random.uniform(100, 5000), 2),
                "currency": random.choice(["USD", "EUR", "GBP"]),
                "impressions": random.randint(1000, 100000),
                "clicks": random.randint(100, 10000),
                "conversions": random.randint(10, 1000),
                "created_at": datetime.combine(spend_date, datetime.min.time()),
            }
        )

    return campaigns

def generate_web_events(customers: list[dict[str, Any]], count: int = 5000) -> list[dict[str, Any]]:
    events = []
    event_types = ["page_view", "add_to_cart", "checkout_start", "payment_info", "purchase"]
    devices = ["desktop", "mobile", "tablet"]
    browsers = ["chrome", "safari", "firefox", "edge"]

    for _ in range(count):
        customer = random.choice(customers)
        session_id = str(uuid.uuid4())
        device = random.choice(devices)
        browser = random.choice(browsers)
        timestamp = fake.date_time_between(start_date="-30d", end_date="now")

        for index, event_type in enumerate(event_types):
            if random.random() > 0.8:
                break
            events.append(
                {
                    "event_id": str(uuid.uuid4()),
                    "session_id": session_id,
                    "customer_id": customer["customer_id"],
                    "event_type": event_type,
                    "page_url": f"https://example.com/{event_type}",
                    "referrer_url": "https://example.com"
                    if index == 0
                    else f"https://example.com/{event_types[index - 1]}",
                    "device_type": device,
                    "browser": browser,
                    "ip_address": fake.ipv4(),
                    "event_timestamp": timestamp + timedelta(seconds=index * 10),
                    "properties": json.dumps(
                        {
                            "user_agent": f"{browser.capitalize()} on {device.capitalize()}",
                            "screen_resolution": f"{random.randint(800, 2000)}x{random.randint(600, 1400)}",
                        }
                    ),
                }
            )
    return events

def generate_web_event_batch(customers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return generate_web_events(customers, 1)

def generate_live_order_activity(
    customers: list[dict[str, Any]], products: list[dict[str, Any]]
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    orders, order_items = generate_orders(customers, products, 1)
    order = orders[0]
    payment = generate_payments([order], 1)[0]
    return order, order_items, payment