"""Round-trip a transformed commits DataFrame through JSON-serializable records.

Airflow's XCom backend (by default) JSON-serializes task return values, and
plain pandas/numpy types (``Timestamp``, ``numpy.int64``, ``numpy.bool_``)
are not JSON-serializable. These helpers convert between the two
representations so DAG tasks can pass a DataFrame's contents through XCom.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from github_etl.transform import OUTPUT_COLUMNS


def dataframe_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert a transformed DataFrame into plain-Python, JSON-safe records."""
    records: list[dict[str, Any]] = []
    for row in df.itertuples(index=False):
        row_dict = row._asdict()
        committed_at = row_dict["committed_at"]
        records.append(
            {
                "sha": row_dict["sha"],
                "author_name": row_dict["author_name"],
                "author_email": row_dict["author_email"],
                "author_login": row_dict["author_login"],
                "committed_at": committed_at.isoformat() if pd.notna(committed_at) else None,
                "message_subject": row_dict["message_subject"],
                "message": row_dict["message"],
                "is_merge_commit": bool(row_dict["is_merge_commit"]),
                "message_length": int(row_dict["message_length"]),
                "url": row_dict["url"],
            }
        )
    return records


def records_to_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    """Reconstruct a DataFrame with the same dtypes ``transform_commits`` produces."""
    if not records:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = pd.DataFrame(records, columns=OUTPUT_COLUMNS)
    df["committed_at"] = pd.to_datetime(df["committed_at"], utc=True, errors="coerce")
    df["author_name"] = df["author_name"].astype(str)
    df["author_email"] = df["author_email"].astype(str)
    df["message_subject"] = df["message_subject"].astype(str)
    df["message"] = df["message"].astype(str)
    df["is_merge_commit"] = df["is_merge_commit"].astype(bool)
    df["message_length"] = df["message_length"].astype(int)
    return df
