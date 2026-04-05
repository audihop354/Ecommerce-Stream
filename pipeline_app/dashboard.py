from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from io import BytesIO

import boto3
import pandas as pd
import pyarrow.parquet as pq
import streamlit as st

from pipeline_app.config import get_settings


MAX_FILES_PER_DATASET = {
    "raw/web_events/": 150,
    "raw/orders/": 80,
    "raw/payments/": 80,
    "raw/customers/": 20,
    "raw/products/": 20,
    "raw/support_tickets/": 40,
    "raw/incidents/": 20,
    "raw/marketing_spend/": 20,
}


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(18, 111, 84, 0.16), transparent 28%),
                radial-gradient(circle at top right, rgba(194, 120, 59, 0.14), transparent 24%),
                linear-gradient(180deg, #f7f2ea 0%, #f4efe6 100%);
        }
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
            max-width: 1300px;
        }
        .hero-panel {
            background: linear-gradient(135deg, #18342d 0%, #255347 55%, #a85d31 100%);
            color: #f8f4ee;
            border-radius: 22px;
            padding: 28px 30px;
            box-shadow: 0 18px 40px rgba(32, 44, 37, 0.18);
            margin-bottom: 1rem;
        }
        .hero-kicker {
            letter-spacing: 0.18em;
            text-transform: uppercase;
            font-size: 0.74rem;
            opacity: 0.78;
            margin-bottom: 0.55rem;
        }
        .hero-title {
            font-size: 2.35rem;
            line-height: 1.05;
            font-weight: 700;
            margin-bottom: 0.5rem;
        }
        .hero-copy {
            font-size: 1rem;
            line-height: 1.6;
            max-width: 820px;
            opacity: 0.9;
        }
        .hero-badges {
            display: flex;
            gap: 0.55rem;
            flex-wrap: wrap;
            margin-top: 1.1rem;
        }
        .hero-badge {
            border: 1px solid rgba(255, 255, 255, 0.18);
            background: rgba(255, 255, 255, 0.1);
            padding: 0.38rem 0.7rem;
            border-radius: 999px;
            font-size: 0.82rem;
        }
        .section-label {
            font-size: 0.78rem;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: #6b746f;
            margin-bottom: 0.3rem;
        }
        .stat-card {
            background: rgba(255, 255, 255, 0.82);
            border: 1px solid rgba(37, 83, 71, 0.12);
            border-radius: 18px;
            padding: 1rem 1rem 0.95rem 1rem;
            box-shadow: 0 10px 24px rgba(54, 62, 58, 0.08);
            min-height: 132px;
        }
        .stat-label {
            color: #6b746f;
            font-size: 0.84rem;
            margin-bottom: 0.5rem;
        }
        .stat-value {
            font-size: 1.95rem;
            line-height: 1;
            font-weight: 700;
            color: #18342d;
            margin-bottom: 0.45rem;
        }
        .stat-subtitle {
            color: #5f554d;
            font-size: 0.88rem;
            line-height: 1.45;
        }
        .panel-title {
            font-size: 1.15rem;
            font-weight: 700;
            color: #18342d;
            margin-bottom: 0.2rem;
        }
        .panel-copy {
            color: #5f675f;
            font-size: 0.92rem;
            margin-bottom: 0.85rem;
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #f5efe7 0%, #f0ebe2 100%);
            border-right: 1px solid rgba(24, 52, 45, 0.08);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def format_number(value: int | float) -> str:
    return f"{value:,.0f}"


def format_currency(value: float) -> str:
    return f"${value:,.2f}"


@st.cache_resource
def get_s3_client():
    settings = get_settings()
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
    )


def list_parquet_keys(prefix: str) -> list[str]:
    settings = get_settings()
    client = get_s3_client()
    paginator = client.get_paginator("list_objects_v2")
    objects: list[dict] = []
    for page in paginator.paginate(Bucket=settings.minio_bucket, Prefix=prefix):
        objects.extend(page.get("Contents", []))

    parquet_objects = [
        item for item in objects if item["Key"].endswith(".parquet")
    ]
    parquet_objects.sort(key=lambda item: item["LastModified"], reverse=True)
    limit = MAX_FILES_PER_DATASET.get(prefix, 30)
    return [item["Key"] for item in parquet_objects[:limit]]


@st.cache_data(ttl=60)
def load_dataset(prefix: str) -> pd.DataFrame:
    settings = get_settings()
    client = get_s3_client()
    keys = list_parquet_keys(prefix)
    if not keys:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for key in keys:
        body = client.get_object(Bucket=settings.minio_bucket, Key=key)["Body"].read()
        table = pq.read_table(BytesIO(body))
        frames.append(table.to_pandas())

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def safe_datetime(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(dtype="datetime64[ns]")
    return pd.to_datetime(frame[column], errors="coerce")


def latest_timestamp(frame: pd.DataFrame, candidate_columns: list[str]) -> str:
    for column in candidate_columns:
        if column in frame.columns:
            series = safe_datetime(frame, column).dropna()
            if not series.empty:
                return series.max().strftime("%Y-%m-%d %H:%M")
    return "n/a"


def filter_by_recent_days(frame: pd.DataFrame, column: str, recent_days: int | None) -> pd.DataFrame:
    if frame.empty or recent_days is None or column not in frame.columns:
        return frame
    timestamps = safe_datetime(frame, column)
    valid = timestamps.dropna()
    if valid.empty:
        return frame
    cutoff = valid.max() - pd.Timedelta(days=recent_days)
    return frame.loc[timestamps >= cutoff].copy()


def render_hero(last_refresh: str, orders: pd.DataFrame, web_events: pd.DataFrame) -> None:
    st.markdown(
        f"""
        <div class="hero-panel">
            <div class="hero-kicker">Streaming Command Center</div>
            <div class="hero-title">E-commerce Pipeline Dashboard</div>
            <div class="hero-copy">
                This dashboard summarizes order flow, payments, site activity, and operational signals in one place.
                Data is read directly from raw parquet files in MinIO so viewers can see what the pipeline is producing without digging through logs or bucket paths.
            </div>
            <div class="hero-badges">
                <div class="hero-badge">Orders: {format_number(len(orders))}</div>
                <div class="hero-badge">Web events: {format_number(len(web_events))}</div>
                <div class="hero-badge">Refresh: {last_refresh}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_stat_card(label: str, value: str, subtitle: str) -> None:
    st.markdown(
        f"""
        <div class="stat-card">
            <div class="stat-label">{label}</div>
            <div class="stat-value">{value}</div>
            <div class="stat-subtitle">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_section_intro(label: str, title: str, copy: str) -> None:
    st.markdown(f'<div class="section-label">{label}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="panel-title">{title}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="panel-copy">{copy}</div>', unsafe_allow_html=True)


def render_overview(orders: pd.DataFrame, payments: pd.DataFrame, customers: pd.DataFrame, products: pd.DataFrame) -> None:
    revenue = float(orders["total_amount"].sum()) if "total_amount" in orders else 0.0
    payment_success = int((payments.get("status", pd.Series(dtype=str)) == "completed").sum())
    total_customers = len(customers)
    total_products = len(products)
    avg_order_value = float(orders["total_amount"].mean()) if not orders.empty and "total_amount" in orders else 0.0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        render_stat_card("Orders", format_number(len(orders)), "Historical rows loaded from raw lake")
    with col2:
        render_stat_card("Revenue", format_currency(revenue), f"Average order value: {format_currency(avg_order_value)}")
    with col3:
        render_stat_card("Successful payments", format_number(payment_success), "Completed payment records in current sample")
    with col4:
        render_stat_card("Customers / Products", f"{format_number(total_customers)} / {format_number(total_products)}", "Dimension coverage available for analysis")


def render_dataset_health(orders: pd.DataFrame, payments: pd.DataFrame, web_events: pd.DataFrame, tickets: pd.DataFrame, incidents: pd.DataFrame) -> None:
    render_section_intro(
        "Coverage",
        "Dataset Health",
        "This table shows which datasets already contain records, which ones are still empty, and the latest available timestamp from each source.",
    )
    health = pd.DataFrame(
        [
            {"dataset": "orders", "rows": len(orders), "latest": latest_timestamp(orders, ["order_date", "_ingested_at", "kafka_timestamp"] )},
            {"dataset": "payments", "rows": len(payments), "latest": latest_timestamp(payments, ["created_at", "updated_at", "_ingested_at"])},
            {"dataset": "web_events", "rows": len(web_events), "latest": latest_timestamp(web_events, ["event_timestamp", "_ingested_at"])},
            {"dataset": "support_tickets", "rows": len(tickets), "latest": latest_timestamp(tickets, ["created_at", "resolved_at", "_ingested_at"])},
            {"dataset": "incidents", "rows": len(incidents), "latest": latest_timestamp(incidents, ["updated_at", "created_at", "_ingested_at"])},
        ]
    )
    st.dataframe(health, use_container_width=True, hide_index=True)


def render_orders_section(orders: pd.DataFrame) -> None:
    render_section_intro(
        "Commerce",
        "Orders",
        "Use this section to review order volume, fulfillment status, and daily revenue trends in the current sample.",
    )
    if orders.empty:
        st.info("No order data found in MinIO yet.")
        return

    status_counts = orders["status"].value_counts().sort_values(ascending=False)
    order_dates = safe_datetime(orders, "order_date")
    revenue_by_day = (
        orders.assign(order_day=order_dates.dt.date)
        .dropna(subset=["order_day"])
        .groupby("order_day", as_index=False)["total_amount"]
        .sum()
        .sort_values("order_day")
    )

    order_value_band = pd.cut(
        orders["total_amount"],
        bins=[0, 100, 500, float("inf")],
        labels=["0-100", "100-500", ">500"],
        include_lowest=True,
    ).value_counts().sort_index()

    col1, col2, col3 = st.columns(3)
    col1.bar_chart(status_counts)
    if not revenue_by_day.empty:
        col2.line_chart(revenue_by_day.set_index("order_day"))
    else:
        col2.info("No valid order timestamps available.")
    col3.bar_chart(order_value_band)


def render_payments_section(payments: pd.DataFrame) -> None:
    render_section_intro(
        "Commerce",
        "Payments",
        "Use this view to check payment success rate and understand how customers are paying across available methods.",
    )
    if payments.empty:
        st.info("No payment data found in MinIO yet.")
        return

    payment_success_rate = 0.0
    if not payments.empty and "status" in payments.columns:
        payment_success_rate = float((payments["status"] == "completed").mean() * 100)

    st.caption(f"Success rate: {payment_success_rate:.1f}%")

    col1, col2 = st.columns(2)
    col1.bar_chart(payments["status"].value_counts().sort_values(ascending=False))
    col2.bar_chart(payments["payment_method"].value_counts().sort_values(ascending=False))


def render_web_events_section(web_events: pd.DataFrame) -> None:
    render_section_intro(
        "Traffic",
        "Web Events",
        "This stream captures user activity sent through Kafka and landed in the raw layer. It helps viewers spot the dominant event types and traffic patterns over time.",
    )
    if web_events.empty:
        st.info("No web event data found in MinIO yet.")
        return

    events = web_events.copy()
    event_times = safe_datetime(events, "event_timestamp")
    events["event_hour"] = event_times.dt.floor("h")
    event_type_counts = events["event_type"].value_counts().sort_values(ascending=False)
    hourly_events = (
        events.dropna(subset=["event_hour"])
        .groupby("event_hour", as_index=False)
        .size()
        .sort_values("event_hour")
        .rename(columns={"size": "events"})
    )

    session_count = web_events["session_id"].nunique() if "session_id" in web_events.columns else 0
    purchase_count = int((web_events["event_type"] == "purchase").sum()) if "event_type" in web_events.columns else 0
    st.caption(f"Sessions: {format_number(session_count)} | Purchase events: {format_number(purchase_count)}")

    col1, col2 = st.columns(2)
    col1.bar_chart(event_type_counts)
    if not hourly_events.empty:
        col2.line_chart(hourly_events.set_index("event_hour"))
    else:
        col2.info("No valid event timestamps available.")


def render_operations_section(tickets: pd.DataFrame, incidents: pd.DataFrame, marketing: pd.DataFrame) -> None:
    render_section_intro(
        "Operations",
        "Support, Incidents, Marketing",
        "This section adds operational context beyond transactions by combining customer support, service reliability, and marketing spend.",
    )
    col1, col2, col3 = st.columns(3)

    if tickets.empty:
        col1.info("No support ticket data.")
    else:
        col1.caption("Support tickets by priority")
        col1.bar_chart(tickets["priority"].value_counts().sort_values(ascending=False))

    if incidents.empty:
        col2.info("No incident data.")
    else:
        col2.caption("Incidents by severity")
        col2.bar_chart(incidents["severity"].value_counts().sort_values(ascending=False))

    if marketing.empty:
        col3.info("No marketing spend data.")
    else:
        col3.caption("Spend by channel")
        spend_by_channel = marketing.groupby("channel", as_index=True)["amount"].sum().sort_values(ascending=False)
        col3.bar_chart(spend_by_channel)


def render_data_preview(name: str, frame: pd.DataFrame) -> None:
    with st.expander(f"Preview: {name}"):
        if frame.empty:
            st.write("No rows available.")
        else:
            st.dataframe(frame.head(20), use_container_width=True)


def build_sidebar_filters(orders: pd.DataFrame, web_events: pd.DataFrame, tickets: pd.DataFrame) -> tuple[int | None, list[str], list[str]]:
    st.sidebar.title("Dashboard Controls")
    recent_window = st.sidebar.selectbox(
        "Time window",
        options=["All", "365 days", "90 days", "30 days", "7 days"],
        index=2,
    )
    recent_days_map = {
        "All": None,
        "365 days": 365,
        "90 days": 90,
        "30 days": 30,
        "7 days": 7,
    }

    order_status_options = sorted(orders["status"].dropna().unique().tolist()) if "status" in orders.columns else []
    event_type_options = sorted(web_events["event_type"].dropna().unique().tolist()) if "event_type" in web_events.columns else []
    ticket_priority_options = sorted(tickets["priority"].dropna().unique().tolist()) if "priority" in tickets.columns else []

    selected_statuses = st.sidebar.multiselect("Order status", order_status_options, default=order_status_options)
    selected_event_types = st.sidebar.multiselect("Event type", event_type_options, default=event_type_options)
    selected_ticket_priorities = st.sidebar.multiselect("Ticket priority", ticket_priority_options, default=ticket_priority_options)

    st.sidebar.caption("These filters only change what is shown on the dashboard. The underlying source remains raw parquet data in MinIO.")
    return recent_days_map[recent_window], selected_statuses, selected_event_types, selected_ticket_priorities


def main() -> None:
    st.set_page_config(page_title="E-commerce Streaming Dashboard", layout="wide")
    inject_styles()

    orders = load_dataset("raw/orders/")
    payments = load_dataset("raw/payments/")
    customers = load_dataset("raw/customers/")
    products = load_dataset("raw/products/")
    web_events = load_dataset("raw/web_events/")
    tickets = load_dataset("raw/support_tickets/")
    incidents = load_dataset("raw/incidents/")
    marketing = load_dataset("raw/marketing_spend/")

    recent_days, selected_statuses, selected_event_types, selected_ticket_priorities = build_sidebar_filters(orders, web_events, tickets)

    orders = filter_by_recent_days(orders, "order_date", recent_days)
    payments = filter_by_recent_days(payments, "created_at", recent_days)
    web_events = filter_by_recent_days(web_events, "event_timestamp", recent_days)
    tickets = filter_by_recent_days(tickets, "created_at", recent_days)
    incidents = filter_by_recent_days(incidents, "created_at", recent_days)
    marketing = filter_by_recent_days(marketing, "spend_date", recent_days)

    if selected_statuses and "status" in orders.columns:
        orders = orders[orders["status"].isin(selected_statuses)]
    if selected_event_types and "event_type" in web_events.columns:
        web_events = web_events[web_events["event_type"].isin(selected_event_types)]
    if selected_ticket_priorities and "priority" in tickets.columns:
        tickets = tickets[tickets["priority"].isin(selected_ticket_priorities)]

    last_refresh = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
    render_hero(last_refresh, orders, web_events)

    render_overview(orders, payments, customers, products)
    render_dataset_health(orders, payments, web_events, tickets, incidents)

    commerce_tab, traffic_tab, ops_tab, preview_tab = st.tabs([
        "Commerce",
        "Traffic",
        "Operations",
        "Preview",
    ])

    with commerce_tab:
        render_orders_section(orders)
        st.markdown("<div style='height: 1rem'></div>", unsafe_allow_html=True)
        render_payments_section(payments)

    with traffic_tab:
        render_web_events_section(web_events)

    with ops_tab:
        render_operations_section(tickets, incidents, marketing)

    with preview_tab:
        render_section_intro(
            "Inspect",
            "Data Preview",
            "This tab shows a small sample of filtered records so viewers can compare the charts with the underlying raw data.",
        )
        render_data_preview("orders", orders)
        render_data_preview("payments", payments)
        render_data_preview("web_events", web_events)
        render_data_preview("support_tickets", tickets)


if __name__ == "__main__":
    main()