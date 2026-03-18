import logging
from pipeline_app.database import create_source_tables, fetch_rows, insert_batch, table_has_rows, wait_for_database
from pipeline_app.generator import (
    generate_customers,
    generate_incidents,
    generate_marketing_spend,
    generate_orders,
    generate_payments,
    generate_products,
    generate_support_tickets,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

def seed_customers_and_products() -> tuple[list[dict], list[dict]]:
    if not table_has_rows("customers"):
        insert_batch("customers", generate_customers(200))

    if not table_has_rows("products"):
        insert_batch("products", generate_products(100))

    customers = fetch_rows(
        """
        SELECT customer_id, first_name, last_name, email, phone, address, city, country, registration_date
        FROM customers
        """
    )
    products = fetch_rows(
        """
        SELECT product_id, name, category, price, cost, stock_quantity, created_at
        FROM products
        """
    )
    return customers, products

def seed_orders_and_dependents(customers: list[dict], products: list[dict]) -> list[dict]:
    if not table_has_rows("orders") or not table_has_rows("order_items"):
        orders, order_items = generate_orders(customers, products, 2000)
        insert_batch("orders", orders)
        insert_batch("order_items", order_items)
    return fetch_rows(
        """
        SELECT order_id, customer_id, order_date, total_amount, status, shipping_address
        FROM orders
        """
    )

def seed_remaining_sources(customers: list[dict], orders: list[dict]) -> None:
    if not table_has_rows("payments"):
        insert_batch("payments", generate_payments(orders, 2500))

    if not table_has_rows("incidents"):
        insert_batch("incidents", generate_incidents(30))

    if not table_has_rows("support_tickets"):
        insert_batch("support_tickets", generate_support_tickets(customers, orders, 300))

    if not table_has_rows("marketing_spend"):
        insert_batch("marketing_spend", generate_marketing_spend(100))

def main() -> None:
    wait_for_database()
    create_source_tables()
    customers, products = seed_customers_and_products()
    orders = seed_orders_and_dependents(customers, products)
    seed_remaining_sources(customers, orders)

if __name__ == "__main__":
    main()