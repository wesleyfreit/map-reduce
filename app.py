import os


class MapReduce:

    def __init__(self, directory):
        self.directory = directory

    def map(self, file):
        word_pairs = []
        with open(file, "r") as f:
            for line in f:
                for word in line.split():
                    word_pairs.append((word, 1))
        return word_pairs

    def reduce(self, word_pairs):
        word_frequency = {}
        for word, count in word_pairs:
            if word not in word_frequency:
                word_frequency[word] = count
            else:
                word_frequency[word] += count
        return word_frequency

    def run(self):
        word_pairs = []

        for file in os.listdir(self.directory):
            if file.endswith(".txt"):
                word_pairs.append(self.map(f"{self.directory}/{file}"))

        word_pairs = [pair for sublist in word_pairs for pair in sublist]

        word_frequency = self.reduce(word_pairs)

        return word_frequency


directory = "./public"

map_reduce = MapReduce(directory)

result = map_reduce.run()

print(result)
