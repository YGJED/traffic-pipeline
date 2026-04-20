"""Send historical traffic from S3 Parquet to Kafka, oldest first.

What it does
One run from --start-time to --end-time. If you omit --end-time, it runs through the
end of that year. Files live at raw/year=Y/month=M/data.parquet. Each Kafka message
is JSON; the key is xd_id.

Optional: after a step that had data, sleep --slice-delay seconds minus how long read
and send took. --run-duration caps total wall-clock time. --emit-mode verbose prints
each step; quiet is the default.

Old design vs this design (short)
Before: load a whole month (or a very big read) into memory, then filter to your dates.
That blew memory (OOM) on large months. Old design also replayed the data infinitely so spark still computes,
but due to append the watermark nothing new was being emitted.

Now: step through the window in small slices (default 10 minutes, --bucket-minutes).
Each slice only loads rows in that time range. PyArrow filters by timestamp while
reading when it can. If that fails for a month file, we load that full month and
filter in pandas (heavy, but only when that fallback runs).

Example walkthrough — replay one hour (2:00 PM to 3:00 PM on a day in January)

Old design (whole month in memory)
  - Open raw/year=2023/month=1/data.parquet.
  - Read the entire month into a big DataFrame in RAM.
  - Filter to rows where measurement_tstamp is between 2:00 PM and 3:00 PM.
  - Sort and send to Kafka.
  - Cost: peak memory is about all of January even though you only needed one hour.
    That is why OOM happened on big months.

New design (small time slices, default 10-minute buckets)
  - Same goal: 2:00 PM to 3:00 PM. Six slices: [2:00,2:10), [2:10,2:20), ... [2:50,3:00).
  - For each slice: open January's file, read rows in that time range (pushdown when
    it works), send to Kafka, then move on.
  - Cost: peak memory is driven by about one slice at a time (roughly 10 minutes of
    data per step, not 30 days).

Command examples:
  python producer.py --start-time 2023-01-01T00:00:00 --end-time 2023-01-01T03:00:00 \\
    --slice-delay 0 --run-duration 600
  python producer.py --start-time 2023-01-01T00:00:00 --end-time 2023-01-01T03:00:00 \\
    --emit-mode verbose --slice-delay 1
"""

import time
import json
import os
import argparse
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import fs as pafs
from kafka import KafkaProducer
from dotenv import load_dotenv, find_dotenv

# =========================
# ARGUMENT PARSING
# =========================


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Replay Parquet from S3 to Kafka (see module docstring).",
    )

    parser.add_argument(
        "--start-time",
        type=str,
        required=True,
        help="ISO start time (e.g. 2023-01-01T00:00:00)",
    )

    parser.add_argument(
        "--end-time",
        type=str,
        default=None,
        help="ISO end time (optional; if omitted, end of start year is used)",
    )

    parser.add_argument(
        "--emit-mode",
        choices=["quiet", "verbose"],
        default="quiet",
        help="quiet: little output. verbose: print each slice window and row counts.",
    )

    parser.add_argument(
        "--slice-delay",
        type=float,
        default=0.0,
        help="After each non-empty slice, wait this many seconds (minus time already spent on read+send).",
    )

    parser.add_argument(
        "--run-duration",
        type=int,
        default=None,
        help="Max runtime in seconds (optional)",
    )

    parser.add_argument(
        "--bucket-minutes",
        type=int,
        default=10,
        help="Length of each time step when reading S3, in minutes (default 10).",
    )

    return parser.parse_args()


args = parse_args()

START_TIME = datetime.fromisoformat(args.start_time)
END_TIME = datetime.fromisoformat(args.end_time) if args.end_time else None
EMIT_MODE = args.emit_mode
SLICE_DELAY_SECONDS = args.slice_delay
RUN_DURATION = args.run_duration
BUCKET_MINUTES = args.bucket_minutes

# =========================
# CONFIGURATION
# =========================

S3_BUCKET = os.getenv("S3_BUCKET","ndot-traffic-pipeline")
# Layout: raw/year=<year>/month=<month>/data.parquet

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "road-segments")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

PARQUET_COLUMNS = [
    "xd_id",
    "measurement_tstamp",
    "speed",
    "reference_speed",
    "confidence_score",
]

# =========================
# Load .env so AWS keys are available for S3
# =========================

load_dotenv(find_dotenv())

# =========================
# KAFKA PRODUCER
# =========================
# JSON value; xd_id is the key so one road stays on one partition. Larger batches
# and a short linger help when replaying busy windows.

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    batch_size=131072,
    linger_ms=20,
    compression_type="lz4",
    acks="all",
)


# =========================
# S3: read one time slice at a time (see module docstring)
# =========================


def _s3_filesystem():
    """PyArrow S3 filesystem; uses AWS credentials from the environment."""
    return pafs.S3FileSystem(region=os.getenv("AWS_REGION", "us-east-1"))


def _object_path(year: int, month: int) -> str:
    """Path to one month file on S3."""
    return f"{S3_BUCKET}/raw/year={year}/month={month}/data.parquet"


def _months_touching_half_open(lo: pd.Timestamp, hi_excl: pd.Timestamp):
    """Which calendar months overlap [lo, hi_excl). hi_excl is not included in the range."""
    if hi_excl <= lo:
        return []
    last = hi_excl - pd.Timedelta(nanoseconds=1)
    months = []
    y, m = lo.year, lo.month
    end_y, end_m = last.year, last.month
    while (y, m) <= (end_y, end_m):
        months.append((y, m))
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return months


def _read_one_month_slice(
    year: int,
    month: int,
    lo: pd.Timestamp,
    hi_excl: pd.Timestamp,
) -> pd.DataFrame:
    """Rows from one month file from lo up to (not including) hi_excl.

    Prefer filtering while reading. On error, load the full month and filter in pandas.
    """
    path = _object_path(year, month)
    filesystem = _s3_filesystem()
    dataset = ds.dataset(path, filesystem=filesystem, format="parquet")
    ts = ds.field("measurement_tstamp")
    filt = (ts >= pa.scalar(lo)) & (ts < pa.scalar(hi_excl))
    try:
        table = dataset.to_table(filter=filt, columns=PARQUET_COLUMNS)
    except (pa.ArrowInvalid, OSError, ValueError):
        # Fallback: whole month into memory, then filter (see module docstring).
        table = dataset.to_table(columns=PARQUET_COLUMNS)
        if table.num_rows == 0:
            return pd.DataFrame(columns=PARQUET_COLUMNS)
        pdf = table.to_pandas()
        pdf["measurement_tstamp"] = pd.to_datetime(pdf["measurement_tstamp"])
        return pdf[(pdf["measurement_tstamp"] >= lo) & (pdf["measurement_tstamp"] < hi_excl)]

    if table.num_rows == 0:
        return pd.DataFrame(columns=PARQUET_COLUMNS)
    pdf = table.to_pandas()
    pdf["measurement_tstamp"] = pd.to_datetime(pdf["measurement_tstamp"])
    return pdf


def read_parquet_bucket(lo: pd.Timestamp, hi_excl: pd.Timestamp, clip_start: pd.Timestamp, clip_end: pd.Timestamp) -> pd.DataFrame:
    """Read rows for one bucket from S3, then clip to the real replay window.

    lo and hi_excl are the bucket on the clock. clip_start and clip_end are the
    user's --start-time and end so the first and last buckets do not spill outside.
    clip_end is inclusive. Missing month files are skipped.
    """
    lo = pd.Timestamp(lo)
    hi_excl = pd.Timestamp(hi_excl)
    dfs = []
    for year, month in _months_touching_half_open(lo, hi_excl):
        try:
            dfs.append(_read_one_month_slice(year, month, lo, hi_excl))
        except FileNotFoundError:
            continue
        except OSError as e:
            if "404" in str(e) or "Not Found" in str(e):
                continue
            raise
    if not dfs:
        return pd.DataFrame(columns=PARQUET_COLUMNS)
    out = pd.concat(dfs, ignore_index=True)
    out = out[(out["measurement_tstamp"] >= clip_start) & (out["measurement_tstamp"] <= clip_end)]
    return out


def effective_end_time() -> datetime:
    """End of replay: --end-time if set, else last second of the start year."""
    if END_TIME is not None:
        return END_TIME
    return datetime(START_TIME.year, 12, 31, 23, 59, 59)


def iter_time_buckets(t0: datetime, t1: datetime, minutes: int):
    """Yield [lo, hi) buckets of length `minutes`, aligned to round clock times.

    The first bucket may start at :00, :10, etc.; lo is never before t0.
    """
    t0 = pd.Timestamp(t0)
    t1 = pd.Timestamp(t1)
    delta = pd.Timedelta(minutes=minutes)
    cur = t0.floor(f"{minutes}min")
    while cur <= t1:
        nxt = cur + delta
        lo = max(cur, t0)
        hi_excl = nxt
        if lo < hi_excl:
            yield lo, hi_excl
        cur = nxt


# =========================
# STREAM TO KAFKA
# =========================


def _send_row(row):
    """Send one row to Kafka; key is xd_id."""
    message = {
        "xd_id": row.xd_id,
        "measurement_tstamp": row.measurement_tstamp.isoformat(),
        "speed": row.speed,
        "reference_speed": row.reference_speed,
        "confidence_score": row.confidence_score,
    }
    producer.send(
        KAFKA_TOPIC,
        key=str(row.xd_id).encode("utf-8"),
        value=message,
    )


def _over_run_duration(start_wall_time):
    """Return True if --run-duration has been exceeded."""
    return RUN_DURATION is not None and (time.time() - start_wall_time > RUN_DURATION)


def _send_dataframe(df: pd.DataFrame, start_wall_time) -> bool:
    """Send rows in time order. Return True if --run-duration stopped the run early."""
    if df.empty:
        return False
    df = df.sort_values("measurement_tstamp")
    for row in df.itertuples(index=False):
        if _over_run_duration(start_wall_time):
            print("Reached run duration limit. Stopping.")
            producer.flush()
            return True
        _send_row(row)
    producer.flush()
    return False


def stream_one_pass(t_end: datetime, start_wall_time) -> None:
    """Walk from START_TIME to t_end: for each bucket, read S3, send Kafka, optional delay.

    Delay runs only after a non-empty bucket; length is --slice-delay minus elapsed work.
    """
    clip_start = pd.Timestamp(START_TIME)
    clip_end = pd.Timestamp(t_end)
    verbose = EMIT_MODE == "verbose"
    n_buckets = 0
    n_nonempty = 0

    for lo, hi_excl in iter_time_buckets(START_TIME, t_end, BUCKET_MINUTES):
        n_buckets += 1
        if verbose:
            months = _months_touching_half_open(lo, hi_excl)
            print(f"Reading slice [{lo}, {hi_excl}) (months={months})")
        try:
            chunk = read_parquet_bucket(lo, hi_excl, clip_start, clip_end)
        except Exception as e:
            print(f"Warning: {lo}: {e}")
            continue

        if chunk.empty:
            if verbose:
                print("  0 rows")
            continue

        n_nonempty += 1
        slice_wall = time.time()
        if _send_dataframe(chunk, start_wall_time):
            return

        elapsed = time.time() - slice_wall
        sleep_time = max(0, SLICE_DELAY_SECONDS - elapsed)
        time.sleep(sleep_time)
        if verbose:
            print(f"  sent {len(chunk)} rows, next slice after {sleep_time:.2f}s")

    if verbose:
        print(f"Finished pass: {n_buckets} bucket(s), {n_nonempty} non-empty.")
    else:
        print("Finished pass (single run).")


def stream_data(t_end: datetime):
    """Replay from START_TIME through t_end in one pass."""
    stream_one_pass(t_end, time.time())


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    stream_data(effective_end_time())
