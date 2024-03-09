import os
import threading


class MapReduce:

    def __init__(self):
        self.input_directory = "./out/files"
        self.output_map_directory = "./out/map"
        self.output_reduce_directory = "./out/reduce"
        self.lock = threading.Lock()

    def map(self, file):
        word_counts: dict = {}

        with open(file, "r") as f:
            for line in f:
                for word in line.split():
                    if word in word_counts:
                        word_counts[word].append(1)
                    else:
                        word_counts[word] = [1]
        with self.lock:
            if not os.path.exists(self.output_map_directory):
                os.makedirs(self.output_map_directory)

        filename, _ = os.path.splitext(os.path.basename(file))

        output_file = os.path.join(
            self.output_map_directory, filename + "-map-output.txt"
        )

        with open(output_file, "w") as f:
            for word, counts in word_counts.items():
                counts_str = ",".join(map(str, counts))
                f.write(f"{word}: [{counts_str}]\n")

    def reduce(self, file):
        word_counts: dict = {}

        with open(file, "r") as f:
            for line in f:
                word, counts_str = line.split(":")
                counts = list(map(int, counts_str.strip()[1:-1].split(",")))
                word_counts[word] = sum(counts)

        with self.lock:
            if not os.path.exists(self.output_reduce_directory):
                os.makedirs(self.output_reduce_directory)

        filename, _ = os.path.splitext(os.path.basename(file))

        filename = filename.replace("-map-output", "")

        output_file = os.path.join(
            self.output_reduce_directory, filename + "-reduce-output.txt"
        )

        with open(output_file, "w") as f:
            for word, count in sorted(word_counts.items()):
                f.write(f"{word}: {count}\n")

    def run_map(self):
        threads: list[threading.Thread] = []

        for file in os.listdir(self.input_directory):
            if file.endswith(".txt"):
                thread = threading.Thread(
                    target=self.map,
                    args=(f"{self.input_directory}/{file}",)
                )

                threads.append(thread)
                thread.start()

        for thread in threads:
            thread.join()

    def run_reduce(self):
        combined_word_counts: dict = {}
        threads: list[threading.Thread] = []

        def process_file(file):
            with open(f"{self.output_map_directory}/{file}", "r") as f:
                for line in f:
                    word, counts_str = line.split(":")
                    counts = list(map(
                        int, counts_str.strip()[1:-1].split(","))
                    )
                    with self.lock:
                        if word in combined_word_counts:
                            combined_word_counts[word] += counts
                        else:
                            combined_word_counts[word] = counts

        for file in os.listdir(self.output_map_directory):
            if file.endswith("-map-output.txt"):
                thread = threading.Thread(target=process_file, args=(file,))
                threads.append(thread)
                thread.start()

        for thread in threads:
            thread.join()

        output_file = os.path.join(
            self.output_reduce_directory, "reduce-output.txt"
        )

        with open(output_file, "w") as f:
            for word, counts in sorted(combined_word_counts.items()):
                f.write(f"{word}: [{sum(counts)}]\n")
