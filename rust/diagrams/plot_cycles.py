import matplotlib.pyplot as plt

def read_data(filename):
    cycles, times = [], []
    with open(filename, 'r') as file:
        for line in file:
            parts = line.split()
            cycles.append(int(parts[1]))
            times.append(int(parts[3]))
    return cycles, times

variants = [('../data/replica_3.txt', '3 Replicas'), ('../data/replica_5.txt', '5 Replicas'), ('../data/replica_10.txt', '10 Replicas')]
colors = ['red', 'blue', 'green']

plt.figure(figsize=(12, 7))

for (filename, name), color in zip(variants, colors):
    cycles, times = read_data(filename)
    plt.plot(cycles, times, marker='o', color=color, label=name)

plt.xlabel('Cycle')
plt.ylabel('Time (ms)')
plt.title('Cycle Times for Different Variants (10 Characters inserted per replica per cycle)')
plt.legend()
plt.grid(True)
plt.show()
