import matplotlib.pyplot as plt
import numpy as np

bytes = [10, 25, 100, 500, 1000]
time1 = [2.859 / 10, 6.561 / 25, 28.005 / 100, 154 / 500, 329 / 1000]
time2 = [2 / 10, 4.8 / 25, 20.005 / 100, 106 / 500, 238 / 1000]

# time1 = [2.225, 2, 6.403, 6.58, 9.044]
# time2 = [3, 1.163, 5, 5.63, 9.33]

plt.figure(figsize=(10, 6))
plt.plot(bytes, time1, '-o', label='Memory Capacity = 50', color='blue')
plt.plot(bytes, time2, '-o', label='Memory Capacity = 100', color='red')

plt.legend(loc='upper left')

plt.xscale('log')
# plt.ylim(-100, 2400)
# plt.ylim(-10, 220)
plt.xlabel('Size of Data (MB)')
plt.ylabel('Latency (sec)')
plt.title('PUT Operation Performances')
# plt.title('GET Operation Performances (10MB reads)')

plt.grid(True, which='both', linestyle='--', linewidth=0.5, alpha=0.7)
plt.tight_layout()
# plt.savefig("unoptimized_exp_put_10.png")
# plt.savefig("unoptimized_exp_put_50.png")
# plt.savefig("unoptimized_exp_get_10.png")
# plt.savefig("unoptimized_exp_get_50.png")
# plt.savefig("unoptimized_exp_put_both.png")
plt.savefig("unoptimized_exp_get_both_put.png")