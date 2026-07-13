"""
DOWNTIME_DETECTOR - Log File Splitter
-----------------------------------------
Splits the raw .log file into two parts, per the lecturer's guidance:

  - First 75% of lines  -> batch_master_data.log
      This becomes the batch layer's "master data" / historical baseline.
      Upload this file DIRECTLY to S3 (no Kinesis involved) -- it's what
      your EMR/PySpark job reads to compute historical error rates and
      average response times per endpoint.

  - Last 25% of lines   -> speed_layer_stream.log
      This is the "new" data that simulates live incoming traffic. ONLY
      this file gets fed through 02_producer.py into Kinesis -> Lambda ->
      DynamoDB (the speed layer).

Usage (run in CloudShell or your terminal, wherever the original .log file is):
    python3 split_log_data.py --file server_logs.log

Produces two files in the same folder:
    batch_master_data.log
    speed_layer_stream.log
"""
import argparse
import sys


def parse_args():
    p = argparse.ArgumentParser(description="Split a .log file into 75% batch / 25% speed-layer portions")
    p.add_argument("--file", required=True, help="Path to the original .log file")
    p.add_argument("--batch-ratio", type=float, default=0.75,
                   help="Fraction of lines to use as batch master data (default 0.75 = 75%%)")
    p.add_argument("--batch-out", default="batch_master_data.log", help="Output filename for batch master data")
    p.add_argument("--speed-out", default="speed_layer_stream.log", help="Output filename for speed-layer stream data")
    return p.parse_args()


def split_file(filepath, batch_ratio, batch_out, speed_out):
    with open(filepath, encoding="utf-8", errors="replace") as f:
        lines = [line for line in f if line.strip()]  # skip blank lines

    total = len(lines)
    if total == 0:
        print("[split] ERROR: input file has no non-empty lines.", file=sys.stderr)
        sys.exit(1)

    split_index = int(total * batch_ratio)

    batch_lines = lines[:split_index]
    speed_lines = lines[split_index:]

    with open(batch_out, "w", encoding="utf-8") as f:
        f.writelines(batch_lines)

    with open(speed_out, "w", encoding="utf-8") as f:
        f.writelines(speed_lines)

    print(f"[split] total lines: {total}")
    print(f"[split] batch master data ({batch_ratio*100:.0f}%): {len(batch_lines)} lines -> {batch_out}")
    print(f"[split] speed layer stream ({(1-batch_ratio)*100:.0f}%): {len(speed_lines)} lines -> {speed_out}")
    print(f"[split] done. Next steps:")
    print(f"  1. Upload '{batch_out}' directly to S3 (this feeds the EMR/PySpark batch job)")
    print(f"  2. Run 02_producer.py against '{speed_out}' ONLY (this feeds Kinesis -> Lambda -> DynamoDB)")


if __name__ == "__main__":
    args = parse_args()
    split_file(args.file, args.batch_ratio, args.batch_out, args.speed_out)
