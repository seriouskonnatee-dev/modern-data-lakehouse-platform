"""
Generates static reference data (products, stores) used by the producer and seeded
directly into the Gold layer as dbt seeds (see gold/seeds/).

Reference data is generated once and reused by every module that needs consistent
product/store/customer IDs, so Bronze events, Silver output, and Gold dimensions all
refer to the same universe of entities.
"""
from __future__ import annotations

import csv
import os
import random
from pathlib import Path

from faker import Faker

from schemas import ReferenceProduct, ReferenceStore

fake = Faker()
Faker.seed(42)
random.seed(42)

CATEGORIES = {
    "Electronics": ["Audio", "Mobile Accessories", "Computing"],
    "Grocery": ["Snacks", "Beverages", "Household"],
    "Apparel": ["Men", "Women", "Kids"],
    "Home & Living": ["Kitchen", "Furniture", "Decor"],
    "Health & Beauty": ["Skincare", "Personal Care", "Wellness"],
}

REGIONS = ["Bangkok", "Central", "North", "Northeast", "South"]
STORE_TYPES = ["hypermarket", "supermarket", "express"]

N_PRODUCTS = 120
N_STORES = 15
N_CUSTOMERS = 2000


def generate_products() -> list[ReferenceProduct]:
    products = []
    for i in range(1, N_PRODUCTS + 1):
        category = random.choice(list(CATEGORIES.keys()))
        subcategory = random.choice(CATEGORIES[category])
        unit_cost = round(random.uniform(20, 2000), 2)
        margin = random.uniform(1.15, 1.8)
        products.append(
            ReferenceProduct(
                product_id=f"PROD-{i:05d}",
                product_name=f"{fake.word().capitalize()} {subcategory} {fake.word().capitalize()}",
                category=category,
                subcategory=subcategory,
                unit_cost=unit_cost,
                list_price=round(unit_cost * margin, 2),
            )
        )
    return products


def generate_stores() -> list[ReferenceStore]:
    stores = []
    for i in range(1, N_STORES + 1):
        region = random.choice(REGIONS)
        stores.append(
            ReferenceStore(
                store_id=f"STORE-{i:02d}",
                store_name=f"{region} {fake.city()} Branch",
                region=region,
                store_type=random.choice(STORE_TYPES),
            )
        )
    return stores


def generate_customer_ids(n: int = N_CUSTOMERS) -> list[str]:
    return [f"CUST-{i:06d}" for i in range(1, n + 1)]


def write_csv(rows: list, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    fieldnames = list(rows[0].__dict__.keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.__dict__)


def main():
    here = Path(__file__).resolve().parent
    out_dir = here.parent / "gold" / "seeds"

    products = generate_products()
    stores = generate_stores()

    write_csv(products, out_dir / "ref_products.csv")
    write_csv(stores, out_dir / "ref_stores.csv")

    customer_ids = generate_customer_ids()
    with open(out_dir / "ref_customer_ids.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id"])
        for cid in customer_ids:
            writer.writerow([cid])

    print(f"Wrote {len(products)} products, {len(stores)} stores, "
          f"{len(customer_ids)} customer ids to {out_dir}")


if __name__ == "__main__":
    main()
