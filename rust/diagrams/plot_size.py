import matplotlib.pyplot as plt

def read_data(filename):
    cycles, times = [], []
    with open(filename, 'r') as file:
        for line in file:
            parts = line.split()
            cycles.append(int(parts[0]))
            times.append(int(parts[1]))
    return cycles, times

variants = [('data/append_document_ref_counts.txt', 'append'), ('data/prepend_document_ref_counts.txt', 'prepend')]
colors = ['red', 'green']

plt.figure(figsize=(12, 7))

for (filename, name), color in zip(variants, colors):
    cycles, times = read_data(filename)
    plt.plot(cycles, times, marker='o', color=color, label=name)

plt.xlabel('Document Length')
plt.ylabel('Amount of ref nodes')
plt.title('Amount of ref nodes when appending characters near the end vs prepending characters near the beginning of a document')
plt.legend()
plt.grid(True)
plt.show()
