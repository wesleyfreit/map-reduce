import os
import threading


class MapReduce:

    def __init__(self):
        self.input_directory = "./public"
        self.output_directory = "./out"

    def map(self, file):
        word_counts: dict = {}

        with open(file, "r") as f:
            for line in f:
                for word in line.split():
                    if word in word_counts:
                        word_counts[word].append(1)
                    else:
                        word_counts[word] = [1]

        if not os.path.exists(f"{self.output_directory}/map"):
            os.makedirs(f"{self.output_directory}/map")

        filename, _ = os.path.splitext(os.path.basename(file))

        output_file = os.path.join(
            self.output_directory, "map", filename + "-map-output.txt"
        )

        with open(output_file, "w") as f:
            for word, counts in word_counts.items():
                counts_str = ",".join(map(str, counts))
                f.write(f"{word}: [{counts_str}]\n")

    # def reduce(self, word_pairs):
    #     word_frequency = {}
    #     for word, count in word_pairs:
    #         if word not in word_frequency:
    #             word_frequency[word] = count
    #         else:
    #             word_frequency[word] += count
    #     return word_frequency

    def run_map(self):
        threads: list[threading.Thread] = []

        for file in os.listdir(self.input_directory):
            if file.endswith(".txt"):
                thread = threading.Thread(
                    target=self.map, args=(f"{self.input_directory}/{file}",)
                )

                threads.append(thread)
                thread.start()

        for thread in threads:
            thread.join()
