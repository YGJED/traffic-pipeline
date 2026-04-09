import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

# =========================
# CONFIG
# =========================

INPUT_CSV = "Davidson-2023-2024-for-NDOT-10-min-Ave.csv"

HIST_OUTPUT_DIR = "inrix_historical_parquet"
STREAM_OUTPUT_DIR = "inrix_stream_parquet"

CHUNK_SIZE = 1_000_000  # adjust based on memory

# Date ranges
HIST_START = "2023-01-01"
HIST_END   = "2023-05-31"

STREAM_START = "2023-06-01"
STREAM_END   = "2023-12-31"

# Columns to keep
COLUMNS = [
    "xd_id",
    "measurement_tstamp",
    "speed",
    "reference_speed",
    "confidence_score"
]

# =========================
# SETUP
# =========================

def reset_output_dirs():
    for path in [HIST_OUTPUT_DIR, STREAM_OUTPUT_DIR]:
        if os.path.exists(path):
            import shutil
            shutil.rmtree(path)
        os.makedirs(path)

# =========================
# PROCESSING
# =========================

def process():

    reset_output_dirs()

    chunk_iter = pd.read_csv(
        INPUT_CSV,
        usecols=COLUMNS,
        parse_dates=["measurement_tstamp"],
        chunksize=CHUNK_SIZE
    )

    total_rows = 0
    hist_rows = 0
    stream_rows = 0

    for i, chunk in enumerate(chunk_iter):
        print(f"\nProcessing chunk {i}...")

        total_rows += len(chunk)

        # =========================
        # Downcast
        # =========================
        chunk["speed"] = chunk["speed"].astype("float32")
        chunk["reference_speed"] = chunk["reference_speed"].astype("float32")
        chunk["confidence_score"] = chunk["confidence_score"].astype("float32")

        # =========================
        # Date filtering
        # =========================
        hist_chunk = chunk[
            (chunk["measurement_tstamp"] >= HIST_START) &
            (chunk["measurement_tstamp"] <= HIST_END)
        ].copy()

        stream_chunk = chunk[
            (chunk["measurement_tstamp"] >= STREAM_START) &
            (chunk["measurement_tstamp"] <= STREAM_END)
        ].copy()

        hist_rows += len(hist_chunk)
        stream_rows += len(stream_chunk)

        # =========================
        # Add partition columns
        # =========================
        if not hist_chunk.empty:
            hist_chunk["year"] = hist_chunk["measurement_tstamp"].dt.year
            hist_chunk["month"] = hist_chunk["measurement_tstamp"].dt.month

            pq.write_to_dataset(
                pa.Table.from_pandas(hist_chunk, preserve_index=False),
                root_path=HIST_OUTPUT_DIR,
                partition_cols=["year", "month"],
                compression="snappy"
            )

        if not stream_chunk.empty:
            stream_chunk["year"] = stream_chunk["measurement_tstamp"].dt.year
            stream_chunk["month"] = stream_chunk["measurement_tstamp"].dt.month

            pq.write_to_dataset(
                pa.Table.from_pandas(stream_chunk, preserve_index=False),
                root_path=STREAM_OUTPUT_DIR,
                partition_cols=["year", "month"],
                compression="snappy"
            )

        # =========================
        # Logging
        # =========================
        print(f"Chunk rows: {len(chunk):,}")
        print(f"Hist rows (this chunk): {len(hist_chunk):,}")
        print(f"Stream rows (this chunk): {len(stream_chunk):,}")
        print(f"Total processed: {total_rows:,}")

    # =========================
    # Final stats
    # =========================
    print("\n===== DONE =====")
    print(f"Total rows processed: {total_rows:,}")
    print(f"Historical rows: {hist_rows:,}")
    print(f"Streaming rows: {stream_rows:,}")


# =========================
# ENTRYPOINT
# =========================

if __name__ == "__main__":
    process()