import json
import logging
import time
from urllib import error, request
from pipeline_app.config import get_settings
logger = logging.getLogger(__name__)

SOURCE_TABLES = [
    "public.customers",
    "public.products",
    "public.orders",
    "public.order_items",
    "public.payments",
    "public.incidents",
    "public.support_tickets",
    "public.marketing_spend",
]

def _json_request(method: str, url: str, payload: dict | None = None) -> tuple[int, str]:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = request.Request(url=url, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=10) as response:
            return response.status, response.read().decode("utf-8")
    except error.HTTPError as http_error:
        return http_error.code, http_error.read().decode("utf-8")
    except error.URLError:
        return 0, ""

def wait_for_debezium(max_retries: int = 30, delay_seconds: int = 3) -> None:
    settings = get_settings()
    for attempt in range(1, max_retries + 1):
        status_code, _ = _json_request("GET", f"{settings.debezium_url}/connectors")
        if status_code == 200:
            logger.info("debezium connect is ready")
            return
        logger.info("waiting for debezium attempt %s/%s", attempt, max_retries)
        time.sleep(delay_seconds)
    raise RuntimeError("debezium connect did not become ready in time")

def build_connector_payload() -> dict:
    settings = get_settings()
    return {
        "name": settings.debezium_connector_name,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": settings.db_host,
            "database.port": str(settings.db_port),
            "database.user": settings.postgres_user,
            "database.password": settings.postgres_password,
            "database.dbname": settings.postgres_db,
            "topic.prefix": settings.debezium_topic_prefix,
            "table.include.list": ",".join(SOURCE_TABLES),
            "plugin.name": "pgoutput",
            "slot.name": "debezium_slot",
            "publication.name": "dbz_publication",
            "snapshot.mode": "initial",
            "decimal.handling.mode": "double",
            "time.precision.mode": "connect",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false",
        },
    }

def ensure_connector_registered() -> None:
    settings = get_settings()
    wait_for_debezium()
    payload = build_connector_payload()

    status_code, _ = _json_request(
        "GET", f"{settings.debezium_url}/connectors/{settings.debezium_connector_name}"
    )
    if status_code == 200:
        update_status, body = _json_request(
            "PUT",
            f"{settings.debezium_url}/connectors/{settings.debezium_connector_name}/config",
            payload["config"],
        )
        if update_status not in (200, 201):
            raise RuntimeError(f"failed to update debezium connector: {body}")
        logger.info("debezium connector updated")
    else:
        create_status, body = _json_request("POST", f"{settings.debezium_url}/connectors", payload)
        if create_status not in (200, 201):
            raise RuntimeError(f"failed to create debezium connector: {body}")
        logger.info("debezium connector created")

def connector_status() -> dict | None:
    settings = get_settings()
    status_code, body = _json_request(
        "GET", f"{settings.debezium_url}/connectors/{settings.debezium_connector_name}/status"
    )
    if status_code != 200 or not body:
        return None
    return json.loads(body)

def wait_for_connector_running(max_retries: int = 20, delay_seconds: int = 3) -> None:
    for attempt in range(1, max_retries + 1):
        status = connector_status()
        if status:
            connector_state = status.get("connector", {}).get("state")
            task_states = [task.get("state") for task in status.get("tasks", [])]
            if connector_state == "RUNNING" and task_states and all(state == "RUNNING" for state in task_states):
                logger.info("debezium connector is running")
                return
        logger.info("waiting for debezium connector state attempt %s/%s", attempt, max_retries)
        time.sleep(delay_seconds)
    raise RuntimeError("debezium connector did not reach RUNNING state")