from azure.eventhub.exceptions import EventDataSendError
from azure.eventhub import EventData, EventHubProducerClient
import paho.mqtt.client as mqtt
import json
import logging
import os
import signal
import ssl
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import threading
from dotenv import load_dotenv

load_dotenv()

# ---------------------------
# Configuration
# ---------------------------

MQTT_HOST = os.getenv("MQTT_HOST", "mqtt.hsl.fi")
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "/hfp/v2/journey/ongoing/vp/+/#")
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "60"))

# Event Hubs auth option 1: connection string
EVENTHUB_CONNECTION_STR = os.getenv("EVENTHUB_CONNECTION_STR", "")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME", "")

# Batching knobs
BATCH_MAX_EVENTS = int(os.getenv("BATCH_MAX_EVENTS", "100"))
BATCH_MAX_SECONDS = float(os.getenv("BATCH_MAX_SECONDS", "2.0"))

# Retry knobs
SEND_RETRY_MAX_ATTEMPTS = int(os.getenv("SEND_RETRY_MAX_ATTEMPTS", "5"))
SEND_RETRY_BASE_SECONDS = float(os.getenv("SEND_RETRY_BASE_SECONDS", "1.0"))
SEND_RETRY_MAX_SECONDS = float(os.getenv("SEND_RETRY_MAX_SECONDS", "30.0"))

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("hsl-hfp-subscriber")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_topic(topic: str) -> Dict[str, Optional[str]]:
    """
    Parse HFP topic into named fields.

    Expected shape:
    /hfp/v2/journey/ongoing/vp/<mode>/<oper>/<veh>/<route>/<dir>/<headsign>/<start>/<next_stop>/<geohash...>

    We keep this tolerant because HFP topics can be extended and the docs recommend using # at the end
    for future compatibility. :contentReference[oaicite:1]{index=1}
    """
    parts = topic.strip("/").split("/")

    result: Dict[str, Optional[str]] = {
        "raw_topic": topic,
        "prefix": None,
        "version": None,
        "journey_type": None,
        "temporal_type": None,
        "event_type": None,
        "transport_mode": None,
        "operator_id": None,
        "vehicle_number": None,
        "route_id": None,
        "direction_id": None,
        "headsign": None,
        "start_time": None,
        "next_stop_id": None,
        "geo_tail": None,
    }

    if len(parts) >= 6:
        result["prefix"] = parts[0]
        result["version"] = parts[1]
        result["journey_type"] = parts[2]
        result["temporal_type"] = parts[3]
        result["event_type"] = parts[4]

    if len(parts) >= 7:
        result["transport_mode"] = parts[5]
    if len(parts) >= 8:
        result["operator_id"] = parts[6]
    if len(parts) >= 9:
        result["vehicle_number"] = parts[7]
    if len(parts) >= 10:
        result["route_id"] = parts[8]
    if len(parts) >= 11:
        result["direction_id"] = parts[9]
    if len(parts) >= 12:
        result["headsign"] = parts[10]
    if len(parts) >= 13:
        result["start_time"] = parts[11]
    if len(parts) >= 14:
        result["next_stop_id"] = parts[12]
    if len(parts) >= 15:
        result["geo_tail"] = "/".join(parts[13:])

    return result


def decode_payload(raw_payload: bytes) -> Dict[str, Any]:
    """
    HSL HFP payloads are UTF-8 JSON messages. :contentReference[oaicite:2]{index=2}
    """
    text = raw_payload.decode("utf-8", errors="replace")
    return json.loads(text)


class EventHubBufferedSender:
    """
    Small buffered sender for Event Hubs.
    Uses EventHubProducerClient, the standard Python producer client for sending events. :contentReference[oaicite:3]{index=3}
    """

    def __init__(
        self,
        connection_str: str,
        eventhub_name: str,
        max_events: int = 100,
        max_seconds: float = 2.0,
        retry_max_attempts: int = 5,
        retry_base_seconds: float = 1.0,
        retry_max_seconds: float = 30.0,
    ) -> None:
        if not connection_str:
            raise ValueError("EVENTHUB_CONNECTION_STR is required")
        if not eventhub_name:
            raise ValueError("EVENTHUB_NAME is required")

        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_str,
            eventhub_name=eventhub_name,
        )
        self.max_events = max_events
        self.max_seconds = max_seconds
        self.retry_max_attempts = retry_max_attempts
        self.retry_base_seconds = retry_base_seconds
        self.retry_max_seconds = retry_max_seconds
        self.buffer: List[str] = []
        self.last_flush = time.monotonic()
        self._lock = threading.Lock()

    def should_flush_by_time(self) -> bool:
        return bool(self.buffer) and (time.monotonic() - self.last_flush) >= self.max_seconds

    def add(self, payload_json: str) -> None:
        should_flush = False
        with self._lock:
            self.buffer.append(payload_json)
            should_flush = len(self.buffer) >= self.max_events

        if should_flush:
            self.flush()

    def _send_batch_with_retry(self, batch) -> None:
        attempt = 0
        while True:
            try:
                self.producer.send_batch(batch)
                return
            except EventDataSendError:
                attempt += 1
                if attempt >= self.retry_max_attempts:
                    logger.exception(
                        "Event Hubs send failed after %d attempt(s)", attempt
                    )
                    raise

                sleep_seconds = min(
                    self.retry_base_seconds * (2 ** (attempt - 1)),
                    self.retry_max_seconds,
                )
                logger.warning(
                    "Transient Event Hubs send failure on attempt %d/%d; retrying in %.1f second(s)",
                    attempt,
                    self.retry_max_attempts,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)

    def flush(self) -> None:
        with self._lock:
            if not self.buffer:
                return
            items_to_send = list(self.buffer)

        batch = self.producer.create_batch()

        try:
            sent_count = 0
            for item in items_to_send:
                event = EventData(item)
                try:
                    batch.add(event)
                except ValueError:
                    # Current batch full; send and start a new one
                    if len(batch) > 0:
                        self._send_batch_with_retry(batch)
                        sent_count += len(batch)
                    batch = self.producer.create_batch()
                    batch.add(event)

            if len(batch) > 0:
                self._send_batch_with_retry(batch)
                sent_count += len(batch)

            with self._lock:
                if self.buffer[: len(items_to_send)] == items_to_send:
                    del self.buffer[: len(items_to_send)]
                else:
                    self.buffer = self.buffer[len(items_to_send):]
                self.last_flush = time.monotonic()

            logger.info("Sent %d event(s) to Event Hubs", sent_count)

        except Exception:
            logger.exception(
                "Event Hubs send failed; retaining %d buffered event(s)", len(
                    items_to_send)
            )
            raise

    def close(self) -> None:
        try:
            self.flush()
        finally:
            self.producer.close()


class HslHfpSubscriber:
    def __init__(self) -> None:
        self.running = True
        self.sender = EventHubBufferedSender(
            connection_str=EVENTHUB_CONNECTION_STR,
            eventhub_name=EVENTHUB_NAME,
            max_events=BATCH_MAX_EVENTS,
            max_seconds=BATCH_MAX_SECONDS,
            retry_max_attempts=SEND_RETRY_MAX_ATTEMPTS,
            retry_base_seconds=SEND_RETRY_BASE_SECONDS,
            retry_max_seconds=SEND_RETRY_MAX_SECONDS,
        )

        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

        # TLS for mqtt.hsl.fi:8883
        self.client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info("Connected to MQTT broker %s:%s", MQTT_HOST, MQTT_PORT)
            logger.info("Subscribing to topic: %s", MQTT_TOPIC)
            client.subscribe(MQTT_TOPIC)
        else:
            logger.error("MQTT connect failed with rc=%s", rc)

    def on_disconnect(self, client, userdata, rc):
        if rc != 0 and self.running:
            logger.warning(
                "Unexpected MQTT disconnect rc=%s; client will retry", rc)
        else:
            logger.info("MQTT disconnected rc=%s", rc)

    def on_message(self, client, userdata, msg):
        try:
            topic_info = parse_topic(msg.topic)
            payload = decode_payload(msg.payload)

            envelope = {
                "ingest_ts_utc": utc_now_iso(),
                "source": "hsl_hfp_mqtt",
                "mqtt": {
                    "host": MQTT_HOST,
                    "port": MQTT_PORT,
                    "topic": msg.topic,
                    "qos": msg.qos,
                    "retain": bool(msg.retain),
                },
                "topic_parsed": topic_info,
                "payload": payload,
            }

            self.sender.add(json.dumps(envelope, ensure_ascii=False))
            logger.debug("Buffered message from topic=%s", msg.topic)

        except json.JSONDecodeError:
            logger.exception(
                "Failed to decode JSON payload for topic=%s", msg.topic)
        except Exception:
            logger.exception(
                "Unexpected processing failure for topic=%s", msg.topic)

    def start(self) -> None:
        logger.info("Starting subscriber")
        self.client.connect(MQTT_HOST, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
        self.client.loop_start()

        try:
            while self.running:
                time.sleep(1)
                # Time-based flush for small volumes
                if self.sender.should_flush_by_time():
                    self.sender.flush()
        finally:
            self.stop()

    def stop(self) -> None:
        if not self.running:
            return
        self.running = False
        logger.info("Stopping subscriber")
        try:
            self.client.loop_stop()
            self.client.disconnect()
        finally:
            self.sender.close()


def main() -> int:
    subscriber = HslHfpSubscriber()

    def handle_signal(signum, frame):
        logger.info("Received signal %s", signum)
        subscriber.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    subscriber.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
