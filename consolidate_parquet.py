import os
import shutil
import pyarrow.parquet as pq
import pyarrow as pa

# =========================
# CONFIG
# =========================

# Directories to consolidate (will process both)
TARGETS = [
    "inrix_historical_parquet",
    "inrix_stream_parquet",
]

# Temp suffix used during rewrite to avoid clobbering originals mid-run
TEMP_SUFFIX = "_consolidating_temp"

# =========================
# HELPERS
# =========================

def find_partition_dirs(root: str) -> list[str]:
    """
    Walk the directory tree and return all leaf directories
    (i.e. dirs that contain .parquet files, not subdirectories).
    e.g. inrix_historical_parquet/year=2023/month=1
    """
    leaves = []
    for dirpath, dirnames, filenames in os.walk(root):
        parquet_files = [f for f in filenames if f.endswith(".parquet")]
        if parquet_files:
            leaves.append((dirpath, parquet_files))
    return leaves


def consolidate_partition(dirpath: str, filenames: list[str]):
    full_paths = [os.path.join(dirpath, f) for f in filenames]

    # prune.py can leave one Parquet per partition or several (chunked writes). Consolidation is
    # not only merging shards: every Hive leaf (year=/month=) should end up as one canonical
    # data.parquet with microsecond timestamps and sorted rows before upload / Spark. We used to
    # bail out when there was already a single file, but that skipped the rewrite — the lone file
    # could still be TIMESTAMP(NANOS) or not named data.parquet, which broke spark.read.parquet on S3.
    # Always run the full read → cast/sort → write path.
    print(f"  Reading {len(full_paths)} file(s) from {dirpath} ...")

    # Read all parquet files in this partition into one table
    table = pq.read_table(full_paths)

    # Downstream Spark (e.g. spark.read.parquet on S3) often fails on nanosecond timestamps in Parquet:
    # errors mention TIMESTAMP(NANOS) or an unsupported / illegal Parquet timestamp type. That happens
    # because pandas/pyarrow sometimes still produce or round-trip timestamp[ns] even when prune tried
    # to standardize; reading many files into one table can preserve ns in the combined schema.
    # If this column is ns, cast it to microseconds here so the single output file is Spark-safe.
    if "measurement_tstamp" in table.schema.names:
        field = table.schema.field("measurement_tstamp")
        if pa.types.is_timestamp(field.type) and getattr(field.type, "unit", None) == "ns":
            col_idx = table.schema.get_field_index("measurement_tstamp")
            table = table.set_column(
                col_idx,
                "measurement_tstamp",
                table.column(col_idx).cast(pa.timestamp("us")),
            )

    # Sort by timestamp so Spark can range-scan efficiently
    if "measurement_tstamp" in table.schema.names:
        import pyarrow.compute as pc
        idx = pc.sort_indices(table, sort_keys=[("measurement_tstamp", "ascending")])
        table = table.take(idx)

    total_rows = len(table)

    # Write to a temp file first (safe rewrite)
    temp_path = os.path.join(dirpath, "__consolidated_temp.parquet")
    pq.write_table(
        table,
        temp_path,
        compression="snappy",
        row_group_size=500_000,
        # Tell the Parquet writer to store timestamps as microseconds (Spark’s happy path). Without this,
        # Arrow may still emit nanosecond logical types in the file even after the in-table cast above.
        coerce_timestamps="us",
        # Coercing ns→us loses digits below microseconds; Arrow would raise unless we explicitly allow that truncation.
        allow_truncated_timestamps=True,
    )

    # Delete the originals
    for f in full_paths:
        os.remove(f)

    # Rename temp → final
    final_path = os.path.join(dirpath, "data.parquet")
    os.rename(temp_path, final_path)

    print(f"  Done — {total_rows:,} rows → {final_path}")


# =========================
# MAIN
# =========================

def consolidate(root: str):
    if not os.path.exists(root):
        print(f"Directory not found, skipping: {root}")
        return

    print(f"\n{'='*60}")
    print(f"Consolidating: {root}")
    print(f"{'='*60}")

    partitions = find_partition_dirs(root)
    if not partitions:
        print("  No parquet files found.")
        return

    print(f"Found {len(partitions)} partition(s)\n")
    for dirpath, filenames in partitions:
        consolidate_partition(dirpath, filenames)

    print(f"\nFinished {root}")


if __name__ == "__main__":
    for target in TARGETS:
        consolidate(target)

    print("\n===== ALL DONE =====")
    print("Each partition now contains a single data.parquet file.")
    print("Spark/PyArrow can still read these using the root directory path —")
    print("partition discovery (year=/month=) works the same regardless of file count.")
