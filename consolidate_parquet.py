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

    if len(full_paths) == 1:
        print(f"  Already 1 file — skipping: {dirpath}")
        return

    print(f"  Reading {len(full_paths)} files from {dirpath} ...")

    # Read all parquet files in this partition into one table
    table = pq.read_table(full_paths)

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
