"""Real-Time LLM Streaming Platform
Kafka -> Flink/Dataflow -> LLM enrichment -> BigQuery pipeline
with back-pressure handling, dead-letter queues, and observability.
"""

from __future__ import annotations
import json
import logging
import time
import hashlib
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque

logger = logging.getLogger(__name__)


@dataclass
class StreamEvent:
    event_id: str
    topic: str
    payload: dict
    timestamp: float = field(default_factory=time.time)
    enriched: bool = False
    error: str = ""
    retries: int = 0

    @classmethod
    def from_kafka_message(cls, msg: dict) -> "StreamEvent":
        payload = json.loads(msg.get("value", "{}"))
        return cls(
            event_id=msg.get("key", hashlib.md5(str(payload).encode()).hexdigest()),
            topic=msg.get("topic", "unknown"),
            payload=payload,
        )


@dataclass
class PipelineConfig:
    project_id: str
    kafka_bootstrap: str = "localhost:9092"
    topics: list[str] = field(default_factory=list)
    bq_dataset: str = "streaming_events"
    bq_table: str = "enriched_events"
    max_workers: int = 8
    batch_size: int = 100
    max_retries: int = 3
    dlq_topic: str = "dead-letter-queue"
    llm_model: str = "gemini-1.5-flash"


class LLMEnricher:
    """Enriches stream events with LLM-generated insights."""

    def __init__(self, model_name: str = "gemini-1.5-flash") -> None:
        self.model_name = model_name
        self._model = None

    def _get_model(self):
        if self._model is None:
            import vertexai.generative_models as genai
            self._model = genai.GenerativeModel(self.model_name)
        return self._model

    def enrich(self, event: StreamEvent, prompt_template: str) -> dict:
        """Generate LLM enrichment for an event."""
        prompt = prompt_template.format(**event.payload)
        try:
            model = self._get_model()
            response = model.generate_content(prompt)
            return {"llm_insight": response.text, "model": self.model_name}
        except Exception as e:
            logger.error("LLM enrichment failed for event %s: %s", event.event_id, e)
            return {"llm_insight": "", "error": str(e)}

    def batch_enrich(
        self, events: list[StreamEvent], prompt_template: str, max_workers: int = 4
    ) -> list[dict]:
        """Parallel batch enrichment."""
        results = [None] * len(events)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.enrich, e, prompt_template): i
                for i, e in enumerate(events)
            }
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    results[idx] = future.result()
                except Exception as e:
                    results[idx] = {"error": str(e)}
        return results


class BackPressureController:
    """Token bucket back-pressure controller for rate limiting."""

    def __init__(self, rate: float = 100.0, burst: int = 200) -> None:
        self.rate = rate
        self.burst = burst
        self._tokens = burst
        self._last_refill = time.monotonic()

    def acquire(self, n: int = 1) -> float:
        """Acquire n tokens, returning wait time in seconds."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
        self._last_refill = now
        if self._tokens >= n:
            self._tokens -= n
            return 0.0
        wait = (n - self._tokens) / self.rate
        self._tokens = 0
        return wait


class DeadLetterQueue:
    """In-memory DLQ with export capability."""

    def __init__(self, max_size: int = 10000) -> None:
        self._queue: deque = deque(maxlen=max_size)

    def put(self, event: StreamEvent, reason: str) -> None:
        self._queue.append({"event": event, "reason": reason, "ts": time.time()})
        logger.warning("DLQ: event %s -> %s", event.event_id, reason)

    def drain(self) -> list[dict]:
        items = list(self._queue)
        self._queue.clear()
        return items

    def __len__(self) -> int:
        return len(self._queue)


class StreamingLLMPipeline:
    """End-to-end real-time pipeline: Kafka -> LLM enrichment -> BigQuery."""

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self.enricher = LLMEnricher(config.llm_model)
        self.back_pressure = BackPressureController()
        self.dlq = DeadLetterQueue()
        self._bq = None
        self._stats = {"processed": 0, "enriched": 0, "dlq": 0, "latency_ms": []}

    def _get_bq(self):
        if self._bq is None:
            from google.cloud import bigquery
            self._bq = bigquery.Client(project=self.config.project_id)
        return self._bq

    def process_batch(
        self,
        events: list[StreamEvent],
        prompt_template: str,
    ) -> dict:
        """Process a batch: enrich + sink to BigQuery."""
        if not events:
            return self._stats.copy()

        wait = self.back_pressure.acquire(len(events))
        if wait > 0:
            logger.info("Back-pressure: sleeping %.2fs", wait)
            time.sleep(wait)

        start = time.perf_counter()
        enrichments = self.enricher.batch_enrich(
            events, prompt_template, self.config.max_workers
        )

        rows, failed = [], []
        for event, enrichment in zip(events, enrichments):
            if enrichment.get("error"):
                event.error = enrichment["error"]
                event.retries += 1
                if event.retries >= self.config.max_retries:
                    self.dlq.put(event, enrichment["error"])
                    self._stats["dlq"] += 1
                else:
                    failed.append(event)
            else:
                event.enriched = True
                rows.append({
                    "event_id": event.event_id,
                    "topic": event.topic,
                    "payload": json.dumps(event.payload),
                    "llm_insight": enrichment.get("llm_insight", ""),
                    "model": enrichment.get("model", ""),
                    "event_ts": event.timestamp,
                    "processed_ts": time.time(),
                })
                self._stats["enriched"] += 1

        if rows:
            self._sink_to_bq(rows)

        latency = (time.perf_counter() - start) * 1000
        self._stats["processed"] += len(events)
        self._stats["latency_ms"].append(latency)
        logger.info(
            "Batch: %d events, %.1fms, DLQ=%d",
            len(events), latency, len(self.dlq)
        )
        return self._stats.copy()

    def _sink_to_bq(self, rows: list[dict]) -> None:
        bq = self._get_bq()
        table_ref = f"{self.config.project_id}.{self.config.bq_dataset}.{self.config.bq_table}"
        errors = bq.insert_rows_json(table_ref, rows)
        if errors:
            logger.error("BQ sink errors: %s", errors)

    def get_metrics(self) -> dict:
        lats = self._stats["latency_ms"]
        return {
            "total_processed": self._stats["processed"],
            "total_enriched": self._stats["enriched"],
            "dlq_size": len(self.dlq),
            "avg_latency_ms": round(sum(lats) / len(lats), 2) if lats else 0,
            "p99_latency_ms": round(sorted(lats)[int(len(lats) * 0.99)], 2) if len(lats) > 100 else 0,
        }

