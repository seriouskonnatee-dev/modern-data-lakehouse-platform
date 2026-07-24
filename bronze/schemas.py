"""
Bronze-layer event schemas, shared by the producer and the Silver cleaning jobs.

These dataclasses are the single source of truth for the Bronze contract described in
docs/design.md section 3.1 / 3.2. Keeping them as plain dataclasses (rather than e.g.
pydantic models) keeps the producer dependency-light, since it's meant to simulate a
lightweight event emitter, not a validation service — validation is Silver's job.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Optional, Literal

SCHEMA_VERSION = "v1"

EventType = Literal["sale", "refund", "void"]
LoyaltyTier = Literal["bronze", "silver", "gold", "platinum"]


@dataclass
class SalesEvent:
    """One raw POS line-item event. Maps to docs/design.md §3.1 raw `sales_events`."""

    event_id: str
    event_type: EventType
    customer_id: Optional[str]
    product_id: str
    store_id: str
    quantity: int
    unit_price: float
    discount_amount: float
    event_ts: str  # ISO-8601, POS-reported time
    ingest_ts: str  # ISO-8601, Bronze landing time
    source_partition: str
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class CustomerProfileSnapshot:
    """One raw customer profile snapshot. Maps to docs/design.md §3.2."""

    customer_id: str
    full_name: str
    email: Optional[str]
    loyalty_tier: LoyaltyTier
    home_store_id: Optional[str]
    snapshot_ts: str  # ISO-8601
    ingest_ts: str  # ISO-8601
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ReferenceProduct:
    """Static product reference data, seeded once (not part of the event stream)."""

    product_id: str
    product_name: str
    category: str
    subcategory: str
    unit_cost: float
    list_price: float


@dataclass
class ReferenceStore:
    """Static store reference data, seeded once (not part of the event stream)."""

    store_id: str
    store_name: str
    region: str
    store_type: str
