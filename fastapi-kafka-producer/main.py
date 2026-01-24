from fastapi import FastAPI
from datetime import datetime, timezone
import uuid, json, random, asyncio
from kafka import KafkaProducer
import signal

app = FastAPI(title="Ecommerce Event Simulation API")

BOOTSTRAP_SERVERS = [
    "localhost:9092",
    "localhost:19092",
    "localhost:29092"
]

TOPIC_MAP = {
    "OrderPaid": "orders.paid.v1",
    "OrderPrepared": "orders.prepared.v1",
    "OrderReady": "orders.ready.v1",
    "OrderCompleted": "orders.completed.v1"
}

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5
)

items_catalog = [
    "Laptop", "Smartphone", "Headphones",
    "Keyboard", "Mouse", "Monitor"
]

order_events = [
    "OrderPaid",
    "OrderPrepared",
    "OrderReady",
    "OrderCompleted"
]

def generate_order_event(order_id: str, event_type: str, topic: str) -> dict:
    return {
        "schema_version": 1,
        "event_type": event_type,
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,

        # ✅ JSON STRING (Postgres-safe)
        "items": json.dumps(
            random.sample(items_catalog, random.randint(1, 3))
        ),

        "amount": round(random.uniform(50, 2000), 2),

        # epoch millis
        "event_ts": int(datetime.now(timezone.utc).timestamp() * 1000),

        "kafka_topic": topic
    }

def send_event(order_id: str, event: dict):
    producer.send(
        topic=event["kafka_topic"],
        key=order_id,
        value=event
    )

simulation_tasks = []

async def simulate_orders(events_per_sec: int):
    interval = 1 / events_per_sec

    while True:
        order_id = str(uuid.uuid4())

        for event_type in order_events:
            topic = TOPIC_MAP[event_type]
            event = generate_order_event(order_id, event_type, topic)
            send_event(order_id, event)
            await asyncio.sleep(random.uniform(0.2, 0.6))

        await asyncio.sleep(interval)

@app.post("/start_simulation")
async def start_simulation(streams: int = 3, events_per_sec: int = 5):
    global simulation_tasks
    for _ in range(streams):
        simulation_tasks.append(
            asyncio.create_task(simulate_orders(events_per_sec))
        )
    return {"status": "started"}

@app.post("/stop_simulation")
async def stop_simulation():
    global simulation_tasks
    for task in simulation_tasks:
        task.cancel()
    simulation_tasks = []
    return {"status": "stopped"}

def shutdown_handler(*_):
    producer.flush()
    producer.close()
    exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
