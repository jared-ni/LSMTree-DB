import matplotlib.pyplot as plt
import numpy as np
import matplotlib.ticker as mticker

# --- Data Preparation --- (Same as before)
def size_to_bytes(size_str):
    size_str = size_str.upper()
    if 'KB' in size_str:
        return float(size_str.replace('KB', '')) * 1024
    if 'MB' in size_str:
        return float(size_str.replace('MB', '')) * 1024**2
    if 'GB' in size_str:
        return float(size_str.replace('GB', '')) * 1024**3
    if 'B' in size_str:
        return float(size_str.replace(' B', '').replace('B', ''))
    return float(size_str)

def time_to_seconds(time_str):
    if not time_str or time_str.strip() == '':
        return np.nan
    minutes = 0
    seconds = 0
    if 'm' in time_str:
        parts = time_str.split('m')
        minutes = float(parts[0])
        seconds_str = parts[1].replace('s', '')
    else:
        seconds_str = time_str.replace('s', '')
    if seconds_str:
        seconds = float(seconds_str)
    return minutes * 60 + seconds

data_sizes_str = ["24 B", "1 MB", "10 MB", "25 MB", "100 MB", "500 MB"]
put_t10_str = ["0m0.295s", "0m1.215s", "0m9.561s", "0m25.142s", "1m34.671s", "8m4.574s"]
get_t10_str = ["0m0.202s", "0m2.453s", "1m44.033s", "11m48.374s", "", ""]
put_t2_str = ["0m0.206s", "0m1.052s", "0m10.318s", "0m25.561s", "1m43.213s", "9m30.431s"]
get_t2_str = ["0m0.209s", "0m2.823s", "1m46.770s", "11m20.816s", "", ""]

data_sizes_bytes = np.array([size_to_bytes(s) for s in data_sizes_str])
put_t10_sec = np.array([time_to_seconds(t) for t in put_t10_str])
get_t10_sec = np.array([time_to_seconds(t) for t in get_t10_str])
put_t2_sec = np.array([time_to_seconds(t) for t in put_t2_str])
get_t2_sec = np.array([time_to_seconds(t) for t in get_t2_str])

# --- Plotting Functions --- (Same format_bytes function)
def format_bytes(x, pos):
    # Check if x is close to zero or negative before log, though log scale avoids zero
    if x <= 0:
        return "0 B" # Or handle as appropriate
    if x < 1024:
        return f'{x:.0f} B'
    elif x < 1024**2:
        # Show .1f for KB if desired, e.g. 1.5KB
        val_kb = x / 1024
        return f'{val_kb:.1f} KB' if val_kb < 10 else f'{val_kb:.0f} KB'
    elif x < 1024**3:
        val_mb = x / 1024**2
        return f'{val_mb:.1f} MB' if val_mb < 10 else f'{val_mb:.0f} MB'
    elif x < 1024**4:
        val_gb = x / 1024**3
        return f'{val_gb:.1f} GB' if val_gb < 10 else f'{val_gb:.0f} GB'
    else:
        val_tb = x / 1024**4
        return f'{val_tb:.1f} TB' if val_tb < 10 else f'{val_tb:.0f} TB'

# --- Graph 1: PUT Performance Comparison (Revised X-axis) ---
plt.figure(figsize=(10, 6))
plt.plot(data_sizes_bytes, put_t10_sec, marker='o', linestyle='-', label='PUT (Size Ratio = 10)')
plt.plot(data_sizes_bytes, put_t2_sec, marker='s', linestyle='--', label='PUT (Size Ratio = 2)')

plt.xscale('log', base=10) # Log scale for X-axis
plt.yscale('linear')

# Apply the formatter, let matplotlib choose tick locations
ax1 = plt.gca()
ax1.xaxis.set_major_formatter(mticker.FuncFormatter(format_bytes))
# ax1.xaxis.set_minor_formatter(mticker.FuncFormatter(format_bytes)) # Optional: format minors too, can get crowded

plt.xlabel("Data Size (Log Scale)")
plt.ylabel("Time (seconds)")
plt.title("Graph 1: PUT Operation Time vs. Data Size")
plt.legend()
plt.grid(True, which="both", ls="--", alpha=0.6)
plt.tight_layout()
plt.savefig("graph1_put_comparison_revised_xaxis.png")
# plt.show()


# --- Graph 2: GET Performance Comparison (Revised X-axis) ---
plt.figure(figsize=(10, 6))
plt.plot(data_sizes_bytes, get_t10_sec, marker='o', linestyle='-', label='GET (Size Ratio = 10)')
plt.plot(data_sizes_bytes, get_t2_sec, marker='s', linestyle='--', label='GET (Size Ratio = 2)')

plt.xscale('log', base=10)
plt.yscale('linear')

ax2 = plt.gca()
ax2.xaxis.set_major_formatter(mticker.FuncFormatter(format_bytes))
# Optional: Adjust x-axis limits if needed, e.g., if the default range is too wide
# plt.xlim(left=data_sizes_bytes[0]*0.8, right=data_sizes_bytes[data_sizes_bytes <= size_to_bytes("25 MB")][-1]*1.2)


plt.xlabel("Data Size (Log Scale)")
plt.ylabel("Time (seconds)")
plt.title("Graph 2: GET Operation Time vs. Data Size")
plt.legend()
plt.grid(True, which="both", ls="--", alpha=0.6)
plt.tight_layout()
plt.savefig("graph2_get_comparison_revised_xaxis.png")
# plt.show()


# --- Graph 3: Size Ratio T=10 Performance (PUT vs GET) (Revised X-axis) ---
plt.figure(figsize=(10, 6))
plt.plot(data_sizes_bytes, put_t10_sec, marker='o', linestyle='-', label='PUT Time (T=10)')
plt.plot(data_sizes_bytes, get_t10_sec, marker='^', linestyle=':', label='GET Time (T=10)')

plt.xscale('log', base=10)
plt.yscale('log') # Log scale for Y-axis

ax3 = plt.gca()
ax3.xaxis.set_major_formatter(mticker.FuncFormatter(format_bytes))
# Optional: Limit x-axis range if needed
# plt.xlim(left=data_sizes_bytes[0]*0.8, right=data_sizes_bytes[~np.isnan(get_t10_sec)][-1]*1.2)


plt.xlabel("Data Size (Log Scale)")
plt.ylabel("Time (seconds) (Log Scale)")
plt.title("Graph 3: Performance with Size Ratio T=10 (PUT vs. GET)")
plt.legend()
plt.grid(True, which="both", ls="--", alpha=0.6)
plt.tight_layout()
plt.savefig("graph3_t10_performance_revised_xaxis.png")
# plt.show()


# --- Graph 4: Size Ratio T=2 Performance (PUT vs GET) (Revised X-axis) ---
plt.figure(figsize=(10, 6))
plt.plot(data_sizes_bytes, put_t2_sec, marker='s', linestyle='--', label='PUT Time (T=2)')
plt.plot(data_sizes_bytes, get_t2_sec, marker='v', linestyle='-.', label='GET Time (T=2)')

plt.xscale('log', base=10)
plt.yscale('log') # Log scale for Y-axis

ax4 = plt.gca()
ax4.xaxis.set_major_formatter(mticker.FuncFormatter(format_bytes))
# Optional: Limit x-axis range if needed
# plt.xlim(left=data_sizes_bytes[0]*0.8, right=data_sizes_bytes[~np.isnan(get_t2_sec)][-1]*1.2)

plt.xlabel("Data Size (Log Scale)")
plt.ylabel("Time (seconds) (Log Scale)")
plt.title("Graph 4: Performance with Size Ratio T=2 (PUT vs. GET)")
plt.legend()
plt.grid(True, which="both", ls="--", alpha=0.6)
plt.tight_layout()
plt.savefig("graph4_t2_performance_revised_xaxis.png")
plt.show()

print("Revised graphs saved as PNG files.")