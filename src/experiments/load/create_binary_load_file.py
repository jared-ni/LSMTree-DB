import struct
import sys

def create_binary_load_file_from_list(key_value_pairs, binary_file_path):
    """
    Takes a list of (key, value) integer tuples and writes them to a binary file
    in the format expected by the LOAD command (4-byte int key, 4-byte int value).

    Args:
        key_value_pairs (list): A list of tuples, where each tuple is (key, value).
                                Both key and value must be integers.
        binary_file_path (str): Path to the output binary file to be created.
    """
    if not isinstance(key_value_pairs, list):
        print("Error: Input must be a list of (key, value) tuples.")
        return False
    if not all(isinstance(pair, tuple) and len(pair) == 2 and
               isinstance(pair[0], int) and isinstance(pair[1], int)
               for pair in key_value_pairs):
        print("Error: Each item in the list must be a tuple of two integers (key, value).")
        return False

    try:
        with open(binary_file_path, 'wb') as outfile:
            count = 0
            for key, value in key_value_pairs:
                try:
                    # Pack as little-endian signed integers (4 bytes each)
                    # 'i' is for signed int (typically 4 bytes)
                    # '<' is for little-endian. Use '>' for big-endian if needed.
                    packed_key = struct.pack('<i', key)
                    packed_value = struct.pack('<i', value)

                    outfile.write(packed_key)
                    outfile.write(packed_value)
                    count += 1
                except struct.error as e:
                    # This error occurs if a number is too large/small for a 4-byte signed int
                    print(f"Warning: Skipping pair ({key}, {value}) due to packing error: {e}. "
                          "Ensure values fit within a 4-byte signed integer range.")
                    continue # Skip this pair and continue with the next

            if count > 0 :
                print(f"Successfully wrote {count} key-value pairs to '{binary_file_path}'.")
            elif not key_value_pairs:
                 print(f"Input list was empty. Created an empty binary file: '{binary_file_path}'.")
            else:
                print(f"No pairs were successfully written to '{binary_file_path}' (all might have been out of range).")
            return True
    except IOError as e:
        print(f"An I/O error occurred while writing to '{binary_file_path}': {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False

if __name__ == "__main__":
    # --- Example Usage ---
    # Define the list of key-value pairs you want to write
    data_to_load = []
    for i in range(8738000):
        data_to_load.append((i, i))

    # Specify the output binary file name
    output_filename = "1000MB_load.bin"

    print(f"Attempting to create '{output_filename}' with the following data:")
    for k, v in data_to_load:
        print(f"  Key: {k}, Value: {v}")
    print("-" * 20)

    if create_binary_load_file_from_list(data_to_load, output_filename):
        print(f"\nTo test with your server, use the command:")
        print(f"db_client > l \"{output_filename}\"")
    else:
        print(f"\nFailed to create the binary file.")

    print("\n--- Example with command-line arguments (optional) ---")
    if len(sys.argv) == 2:
        output_bin_arg = sys.argv[1]
        # For command-line, you'd typically read pairs from a file or generate them.
        # This example uses a fixed list for simplicity if only output path is given.
        print(f"Writing pre-defined example data to '{output_bin_arg}' as specified by argument.")
        create_binary_load_file_from_list(data_to_load, output_bin_arg)
    elif len(sys.argv) == 1 and __name__ == "__main__": # Only print usage if no args and script is run directly
        pass # Handled by the example usage above
    elif len(sys.argv) > 1 :
        print("Usage (for example execution): python script_name.py [output_binary_file_name]")
        print("If output_binary_file_name is provided, the script uses a predefined list of pairs.")
        print("Modify the 'data_to_load' list in the script for custom data when run this way.")