from __future__ import annotations

import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd


CAMPAIGN_MAP = {
    "leads_autos_q2": ("Leads Autos Q2", "Lead generation"),
    "lead_concesionario": ("Lead Concesionario", "Lead generation"),
    "retargeting_sitio": ("Retargeting Sitio", "Retargeting / lead generation"),
    "awareness_marca": ("Awareness Marca", "Awareness"),
    "brand_hashtag": ("Brand Hashtag", "Awareness / engagement"),
    "spark_awareness": ("Spark Awareness", "Awareness / engagement"),
    "programmatic_lead": ("Programmatic Lead", "Lead generation"),
    "prog_lead": ("Programmatic Lead", "Lead generation"),
    "video_reach": ("Video Reach", "Awareness / reach"),
}


PLATFORM_CONFIG = {
    "Meta": {
        "file_pattern": "meta*.xlsx",
        "account_col": "account",
        "sub_entity_col": "adset_name",
        "sub_entity_type": "adset",
    },
    "TikTok": {
        "file_pattern": "tiktok*.xlsx",
        "account_col": "account",
        "sub_entity_col": "adgroup_name",
        "sub_entity_type": "adgroup",
    },
    "DV360": {
        "file_pattern": "dv360*.xlsx",
        "account_col": "advertiser",
        "sub_entity_col": "line_item",
        "sub_entity_type": "line_item",
    },
}


OUTPUT_COLUMNS = [
    "date",
    "platform",
    "account",
    "campaign_raw",
    "campaign_key",
    "campaign_normalized",
    "campaign_objective",
    "sub_entity_type",
    "sub_entity_name",
    "insertion_order",
    "impressions",
    "clicks",
    "spend_original",
    "currency",
    "fx_rate_to_usd",
    "spend_usd",
    "conversions_raw",
    "conversions",
    "conversion_handling",
    "is_lead_comparable",
    "data_quality_flag",
    "source_file",
]


def normalize_key(value: object) -> str:
    """Normalize text enough to join campaign variants across platforms."""
    text = "" if pd.isna(value) else str(value)
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def title_from_key(key: str) -> str:
    if not key:
        return "Unknown Campaign"
    return " ".join(part.capitalize() for part in key.split("_"))


def campaign_lookup(campaign_name: object) -> tuple[str, str, str]:
    key = normalize_key(campaign_name)
    normalized, objective = CAMPAIGN_MAP.get(key, (title_from_key(key), "Unmapped"))
    return key, normalized, objective


def find_one_file(raw_dir: Path, pattern: str) -> Path:
    matches = sorted(raw_dir.glob(pattern))
    matches = [path for path in matches if not path.name.startswith("~$")]
    if len(matches) != 1:
        match_list = ", ".join(path.name for path in matches) or "none"
        raise FileNotFoundError(
            f"Expected exactly one file for pattern {pattern!r} in {raw_dir}. Found: {match_list}"
        )
    return matches[0]


def read_platform_file(platform: str, path: Path, fx_rates: dict[str, float]) -> pd.DataFrame:
    config = PLATFORM_CONFIG[platform]
    df = pd.read_excel(path)
    df.columns = [str(col).strip() for col in df.columns]

    required_cols = [
        "date",
        config["account_col"],
        "campaign_name",
        config["sub_entity_col"],
        "impressions",
        "clicks",
        "spend",
        "conversions",
        "currency",
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"{path.name} is missing required columns: {missing_cols}")

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df["date"], errors="coerce")
    out["platform"] = platform
    out["account"] = df[config["account_col"]].astype("string").str.strip()
    out["campaign_raw"] = df["campaign_name"].astype("string").str.strip()

    lookup = out["campaign_raw"].apply(campaign_lookup)
    out["campaign_key"] = lookup.apply(lambda item: item[0])
    out["campaign_normalized"] = lookup.apply(lambda item: item[1])
    out["campaign_objective"] = lookup.apply(lambda item: item[2])

    out["sub_entity_type"] = config["sub_entity_type"]
    out["sub_entity_name"] = df[config["sub_entity_col"]].astype("string").str.strip()
    out["insertion_order"] = (
        df["insertion_order"].astype("string").str.strip()
        if "insertion_order" in df.columns
        else pd.NA
    )

    out["impressions"] = pd.to_numeric(df["impressions"], errors="coerce").fillna(0).astype(int)
    out["clicks"] = pd.to_numeric(df["clicks"], errors="coerce").fillna(0).astype(int)
    out["spend_original"] = pd.to_numeric(df["spend"], errors="coerce")
    out["currency"] = df["currency"].astype("string").str.upper().str.strip()
    out["fx_rate_to_usd"] = out["currency"].map(fx_rates)
    out["spend_usd"] = out["spend_original"] / out["fx_rate_to_usd"]

    out["conversions_raw"] = pd.to_numeric(df["conversions"], errors="coerce")
    out["conversions"] = out["conversions_raw"].fillna(0)
    out["conversion_handling"] = np.where(
        out["conversions_raw"].isna(),
        "missing_treated_as_zero",
        "reported_value",
    )
    out["is_lead_comparable"] = out["campaign_objective"].str.contains(
        "lead", case=False, na=False
    )
    out["source_file"] = path.name

    flags = pd.Series("", index=out.index, dtype="object")
    flags = add_flag(flags, out["date"].isna(), "invalid_date")
    flags = add_flag(flags, out["spend_original"].isna(), "missing_spend")
    flags = add_flag(flags, out["fx_rate_to_usd"].isna(), "missing_fx_rate")
    flags = add_flag(flags, out["conversions_raw"].isna(), "missing_conversions_treated_as_zero")
    flags = add_flag(flags, out["campaign_objective"].eq("Unmapped"), "unmapped_campaign_name")
    out["data_quality_flag"] = flags

    return out[OUTPUT_COLUMNS]


def add_flag(flags: pd.Series, condition: pd.Series, flag: str) -> pd.Series:
    flags = flags.fillna("").astype("object").copy()
    condition = condition.fillna(False)
    empty_mask = flags.eq("")
    nonempty_mask = ~empty_mask
    flags.loc[condition & empty_mask] = flag
    flags.loc[condition & nonempty_mask] = flags.loc[condition & nonempty_mask] + ";" + flag
    return flags


def add_duplicate_flags(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    grain = [
        "date",
        "platform",
        "account",
        "campaign_raw",
        "sub_entity_name",
        "insertion_order",
    ]
    duplicate_mask = df.duplicated(grain, keep=False)
    df["data_quality_flag"] = add_flag(
        df["data_quality_flag"].fillna(""),
        duplicate_mask,
        "duplicate_natural_grain",
    )
    return df


def build_unified_dataset(raw_dir: Path, fx_rates: dict[str, float]) -> pd.DataFrame:
    frames = []
    for platform, config in PLATFORM_CONFIG.items():
        source_file = find_one_file(raw_dir, config["file_pattern"])
        frames.append(read_platform_file(platform, source_file, fx_rates))
    unified = pd.concat(frames, ignore_index=True)
    unified = add_duplicate_flags(unified)
    unified = unified.sort_values(
        ["date", "platform", "campaign_normalized", "sub_entity_name"],
        na_position="last",
    ).reset_index(drop=True)
    return unified


def build_summary_tables(unified: pd.DataFrame) -> dict[str, pd.DataFrame]:
    platform_summary = (
        unified.groupby("platform", dropna=False)
        .agg(
            spend_usd=("spend_usd", "sum"),
            conversions=("conversions", "sum"),
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
            rows=("platform", "size"),
        )
        .reset_index()
    )
    platform_summary["cpa_usd"] = platform_summary["spend_usd"] / platform_summary[
        "conversions"
    ].replace(0, np.nan)
    platform_summary["ctr"] = platform_summary["clicks"] / platform_summary[
        "impressions"
    ].replace(0, np.nan)

    campaign_summary = (
        unified.groupby(["campaign_normalized", "campaign_objective", "platform"], dropna=False)
        .agg(
            spend_usd=("spend_usd", "sum"),
            conversions=("conversions", "sum"),
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
            rows=("platform", "size"),
        )
        .reset_index()
    )
    campaign_summary["cpa_usd"] = campaign_summary["spend_usd"] / campaign_summary[
        "conversions"
    ].replace(0, np.nan)

    daily_summary = (
        unified.groupby(["date", "platform"], dropna=False)
        .agg(spend_usd=("spend_usd", "sum"), conversions=("conversions", "sum"))
        .reset_index()
    )
    daily_summary["cpa_usd"] = daily_summary["spend_usd"] / daily_summary[
        "conversions"
    ].replace(0, np.nan)

    quality_summary = (
        unified.assign(
            data_quality_flag=unified["data_quality_flag"].replace("", "ok").str.split(";")
        )
        .explode("data_quality_flag")
        .groupby(["platform", "data_quality_flag"], dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["platform", "rows"], ascending=[True, False])
    )

    return {
        "platform_summary": platform_summary,
        "campaign_summary": campaign_summary,
        "daily_summary": daily_summary,
        "quality_summary": quality_summary,
    }


def export_outputs(unified: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    excel_path = output_dir / "unified_campaign_data.xlsx"
    csv_path = output_dir / "unified_campaign_data.csv"

    summaries = build_summary_tables(unified)
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        unified.to_excel(writer, sheet_name="unified_data", index=False)
        for sheet_name, table in summaries.items():
            table.to_excel(writer, sheet_name=sheet_name, index=False)

    unified.to_csv(csv_path, index=False)
    print(f"Wrote {excel_path}")
    print(f"Wrote {csv_path}")
    print(f"Rows appended: {len(unified):,}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize campaign names and append Meta, TikTok, and DV360 exports."
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "Raw_Data",
        help="Folder containing the platform Excel exports.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "Processed_Data",
        help="Folder where normalized outputs will be written.",
    )
    parser.add_argument(
        "--gtq-per-usd",
        type=float,
        default=7.8,
        help="FX rate used to convert GTQ spend into USD. Default: 7.8.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fx_rates = {
        "USD": 1.0,
        "GTQ": args.gtq_per_usd,
    }
    unified = build_unified_dataset(args.raw_dir, fx_rates)
    export_outputs(unified, args.output_dir)


if __name__ == "__main__":
    main()
