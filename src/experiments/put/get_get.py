#!/usr/bin/env python3

import sys
import os

def transform_put_to_get(input_filename, output_filename, max_lines=436900):
    """
    Reads the first 'max_lines' from an input file with lines like 'p <key> <value>',
    extracts the key, and writes 'g <key>' lines to the output file.
    """
    print(f"Reading first {max_lines} lines from: {input_filename}")
    print(f"Writing to: {output_filename}")

    lines_processed = 0
    lines_written = 0

    try:
        with open(input_filename, 'r') as infile, open(output_filename, 'w') as outfile:
            for line in infile:
                if lines_processed >= max_lines:
                    break  # Stop after processing max_lines

                lines_processed += 1
                stripped_line = line.strip()

                if not stripped_line:
                    continue

                parts = stripped_line.split()
                if len(parts) >= 2 and parts[0] == 'p':
                    key = parts[1]
                    outfile.write(f"g {key}\n")
                    lines_written += 1

        print(f"Finished processing.")
        print(f"Total lines read: {lines_processed}")
        print(f"Lines written (g commands): {lines_written}")

    except FileNotFoundError:
        print(f"Error: Input file '{input_filename}' not found.", file=sys.stderr)
        sys.exit(1)
    except IOError as e:
        print(f"Error reading/writing file: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script_name.py <filename_prefix>", file=sys.stderr)
        print("Example: python script_name.py workloadA", file=sys.stderr)
        sys.exit(1)

    prefix = sys.argv[1]
    input_file = f"{prefix}_put.txt"
    output_file = f"{prefix}_get.txt"

    transform_put_to_get(input_file, output_file)