import matplotlib.pyplot as plt
import os
import sys

def read_data_from_file(filename):
    lengths = []
    latencies = []
    with open(filename, 'r') as file:
        for line in file:
            length, latency = map(int, line.strip().split())
            lengths.append(length)
            latencies.append(latency)
    return lengths, latencies

def plot_multiple_files(files):
    plt.figure(figsize=(10, 6))

    for filepath in files:
        filename = os.path.basename(filepath)
        lengths, latencies = read_data_from_file(filepath)
        plt.scatter(lengths, latencies, label=filename, alpha=0.7)

    plt.xlabel('Document Length')
    plt.ylabel('Latency (ms)')
    plt.title('Document Length vs Latency for Multiple Files')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig('length_vs_latency_plot.png')
    plt.show()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script_name.py file1.txt file2.txt file3.txt ...")
        sys.exit(1)

    files = sys.argv[1:]  # Get all command-line arguments except the script name
    plot_multiple_files(files)
