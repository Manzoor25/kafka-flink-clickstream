"""Deterministic 24-SKU e-commerce product catalogue and temporal price-change helpers."""

from __future__ import annotations

import random
from typing import Any

INITIAL_CATALOGUE: list[dict[str, Any]] = [
    {
        "product_id": "SKU-1001",
        "product_name": "AeroPro ANC Headphones",
        "category": "Audio",
        "price": 249.00,
        "weight": 14.0,
    },
    {
        "product_id": "SKU-1002",
        "product_name": "UltraWide 34 OLED Monitor",
        "category": "Electronics",
        "price": 799.00,
        "weight": 9.5,
    },
    {
        "product_id": "SKU-1003",
        "product_name": "KeyChrono Mechanical Tactile",
        "category": "Home Office",
        "price": 139.00,
        "weight": 13.0,
    },
    {
        "product_id": "SKU-1004",
        "product_name": "PulseTrack Titanium Watch",
        "category": "Wearables",
        "price": 329.00,
        "weight": 11.5,
    },
    {
        "product_id": "SKU-1005",
        "product_name": "ThunderDock USB4 Hub",
        "category": "Accessories",
        "price": 189.00,
        "weight": 10.0,
    },
    {
        "product_id": "SKU-1006",
        "product_name": "ErgoLift Standing Desk Frame",
        "category": "Home Office",
        "price": 459.00,
        "weight": 6.5,
    },
    {
        "product_id": "SKU-1007",
        "product_name": "NovaStream 4K Webcam",
        "category": "Home Office",
        "price": 149.00,
        "weight": 9.0,
    },
    {
        "product_id": "SKU-1008",
        "product_name": "SonicBuds Wireless Mini",
        "category": "Audio",
        "price": 99.00,
        "weight": 15.0,
    },
    {
        "product_id": "SKU-1009",
        "product_name": "HyperGlide Pro Gaming Mouse",
        "category": "Gaming",
        "price": 89.00,
        "weight": 12.5,
    },
    {
        "product_id": "SKU-1010",
        "product_name": "ApexForce RTX Handheld",
        "category": "Gaming",
        "price": 649.00,
        "weight": 7.0,
    },
    {
        "product_id": "SKU-1011",
        "product_name": "GaNFast 140W Travel Charger",
        "category": "Accessories",
        "price": 69.00,
        "weight": 14.5,
    },
    {
        "product_id": "SKU-1012",
        "product_name": "StudioCast XLR Condenser Mic",
        "category": "Audio",
        "price": 179.00,
        "weight": 8.0,
    },
    {
        "product_id": "SKU-1013",
        "product_name": "MagSnap Braided USB-C Cable",
        "category": "Accessories",
        "price": 24.00,
        "weight": 16.0,
    },
    {
        "product_id": "SKU-1014",
        "product_name": "LumiBar Monitor Light Pro",
        "category": "Home Office",
        "price": 109.00,
        "weight": 8.5,
    },
    {
        "product_id": "SKU-1015",
        "product_name": "VeloFit Smart Recovery Ring",
        "category": "Wearables",
        "price": 299.00,
        "weight": 7.5,
    },
    {
        "product_id": "SKU-1016",
        "product_name": "PocketSSD NVMe 2TB Rugged",
        "category": "Electronics",
        "price": 199.00,
        "weight": 10.5,
    },
    {
        "product_id": "SKU-1017",
        "product_name": "MeshCore Wi-Fi 7 Tri-Band",
        "category": "Electronics",
        "price": 399.00,
        "weight": 6.0,
    },
    {
        "product_id": "SKU-1018",
        "product_name": "CloudRest Memory Wrist Pad",
        "category": "Accessories",
        "price": 29.00,
        "weight": 11.0,
    },
    {
        "product_id": "SKU-1019",
        "product_name": "ForceFeed Direct-Drive Wheel",
        "category": "Gaming",
        "price": 549.00,
        "weight": 4.5,
    },
    {
        "product_id": "SKU-1020",
        "product_name": "CinemaBar Dolby Soundbar",
        "category": "Audio",
        "price": 429.00,
        "weight": 5.5,
    },
    {
        "product_id": "SKU-1021",
        "product_name": "SolarPack 27000mAh PowerBank",
        "category": "Accessories",
        "price": 119.00,
        "weight": 9.0,
    },
    {
        "product_id": "SKU-1022",
        "product_name": "CreatorPad Mini Macro Deck",
        "category": "Home Office",
        "price": 129.00,
        "weight": 7.5,
    },
    {
        "product_id": "SKU-1023",
        "product_name": "VisionGlass AR Display",
        "category": "Wearables",
        "price": 499.00,
        "weight": 5.0,
    },
    {
        "product_id": "SKU-1024",
        "product_name": "ProBook 16 Workstation Dock",
        "category": "Electronics",
        "price": 279.00,
        "weight": 6.5,
    },
]

CATALOGUE_BY_ID: dict[str, dict[str, Any]] = {p["product_id"]: dict(p) for p in INITIAL_CATALOGUE}


def generate_price_mutation(
    rng: random.Random,
    current_catalogue: dict[str, dict[str, Any]],
    updated_at_ms: int,
) -> dict[str, Any]:
    """Selects a SKU and applies a realistic promotional discount or restock price adjustment."""
    sku = rng.choice(list(current_catalogue.keys()))
    entry = current_catalogue[sku]
    base_price = float(CATALOGUE_BY_ID[sku]["price"])
    multiplier = rng.choice([0.85, 0.90, 0.95, 1.00, 1.05, 1.10])
    new_price = round(base_price * multiplier, 2)
    entry["price"] = new_price
    updated = {
        "product_id": entry["product_id"],
        "product_name": entry["product_name"],
        "category": entry["category"],
        "price": new_price,
        "updated_at_ms": updated_at_ms,
    }
    return updated
