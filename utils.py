import os
import gzip
import shutil
import random
import requests
from typing import Dict, Any, Iterable

import pandas as pd

# --------- CONSTANTS ---------
DATA_DIR = "temp"
DATASETS = {
    "ratings": {
        "gz": "title.ratings.tsv.gz",
        "url": "https://datasets.imdbws.com/title.ratings.tsv.gz",
    },
    "basics": {
        "gz": "title.basics.tsv.gz",
        "url": "https://datasets.imdbws.com/title.basics.tsv.gz",
    },
    "crew": {
        "gz": "title.crew.tsv.gz",
        "url": "https://datasets.imdbws.com/title.crew.tsv.gz",
    },
}

# --------- INTERNAL STATE ---------
_metadata_df: pd.DataFrame | None = None  # lazily built, merged dataframe
_remaining_ids: list[str] = []  # ids left to sample from

# --------- HELPERS ---------

def _ensure_dir() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)


def _download_dataset(name: str) -> None:
    """Download <name>.gz if missing."""
    cfg = DATASETS[name]
    gz_path = os.path.join(DATA_DIR, cfg["gz"])
    if os.path.exists(gz_path):
        return
    print(f"⬇️  Downloading {cfg['gz']} …")
    with requests.get(cfg["url"], stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(gz_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print("   done.")


def _extract_dataset(name: str) -> str:
    """Extract .gz → .tsv if missing; return TSV path."""
    cfg = DATASETS[name]
    gz_path = os.path.join(DATA_DIR, cfg["gz"])
    tsv_path = gz_path.replace(".gz", "")
    if os.path.exists(tsv_path):
        return tsv_path
    print(f"🗜️  Extracting {cfg['gz']} …")
    with gzip.open(gz_path, "rb") as f_in, open(tsv_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    print("   done.")
    return tsv_path


def _load_dataframe(name: str, usecols: Iterable[str] | None = None) -> pd.DataFrame:
    """Ensure dataset present → return DataFrame."""
    _ensure_dir()
    _download_dataset(name)
    tsv_path = _extract_dataset(name)
    return pd.read_csv(
        tsv_path,
        sep="\t",
        na_values="\\N",
        low_memory=False,
        usecols=usecols,
    )

# --------- PUBLIC API ---------

def load_metadata(min_votes: int = 0) -> pd.DataFrame:
    """Load & cache merged ratings+basics+crew, filtered by min_votes."""
    global _metadata_df
    if _metadata_df is not None:
        return _metadata_df.copy()

    print("📖  Loading IMDb metadata …")

    ratings = _load_dataframe(
        "ratings", usecols=["tconst", "averageRating", "numVotes"]
    )
    basics = _load_dataframe(
        "basics",
        usecols=[
            "tconst",
            "titleType",
            "primaryTitle",
            "originalTitle",
            "isAdult",
            "startYear",
            "endYear",
            "runtimeMinutes",
            "genres",
        ],
    )
    crew = _load_dataframe("crew", usecols=["tconst", "directors", "writers"])

    df = (
        ratings.merge(basics, on="tconst", how="left")
        .merge(crew, on="tconst", how="left")
        .rename(columns={"tconst": "movie_id"})
    )

    if min_votes > 0:
        df = df[df["numVotes"] >= min_votes]

    _metadata_df = df.reset_index(drop=True)
    print(f"   {len(_metadata_df):,} rows ready.")
    return _metadata_df.copy()


def get_random_movie_id(min_votes: int = 0) -> str:
    """Return random movie_id with ≥min_votes (unique per run)."""
    global _remaining_ids
    if not _remaining_ids:
        md = load_metadata(min_votes)
        _remaining_ids = md["movie_id"].tolist()
        random.shuffle(_remaining_ids)
    if not _remaining_ids:
        raise RuntimeError("No movie IDs left that satisfy the threshold.")
    return _remaining_ids.pop()


def get_metadata(movie_id: str) -> Dict[str, Any]:
    md = load_metadata()
    row = md.loc[md["movie_id"] == movie_id]
    if row.empty:
        return {}
    return row.iloc[0].to_dict()
