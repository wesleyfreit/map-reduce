from io import TextIOWrapper
import os
import threading


class MapReduce:

    def __init__(self):
        self.input_dict = "./out/dict.txt"
        self.input_directory = "./out/files"
        self.output_map_directory = "./out/map"
        self.output_reduce_directory = "./out/reduce"

    def map(self, file: str, map_write: TextIOWrapper):
        with open(file, "r") as f:
            for line in f:
                for word in line.split():
                    map_write.write(f"{word}: [1]\n")

    def reduce(self, temp_map: str, reduce_write: TextIOWrapper):
        combined_word_counts: dict = {}

        with open(temp_map, "r") as f:
            for line in f:
                word, count_str = line.split(":")
                count = int(count_str.strip(" []\n"))

                if word in combined_word_counts:
                    combined_word_counts[word] += count
                else:
                    combined_word_counts[word] = count

        reduce_write.write(
            "\n".join(
                [
                    f"{word}: [{count}]"
                    for word, count in sorted(combined_word_counts.items())
                ]
            )
        )

    def execute(self):
        if not os.path.exists(self.output_map_directory):
            os.makedirs(self.output_map_directory)

        threads: list[threading.Thread] = []

        temp_map = f"{self.output_map_directory}/temp_map.txt"

        if os.path.exists(temp_map):
            os.remove(temp_map)

        map_write = open(temp_map, "a")

        for file in os.listdir(self.input_directory):
            if file.endswith(".txt"):
                file = f"{self.input_directory}/{file}"

                thread = threading.Thread(
                    target=self.map, args=(file, map_write)
                )

                threads.append(thread)
                thread.start()

        for thread in threads:
            thread.join()

        map_write.close()

        if not os.path.exists(self.output_reduce_directory):
            os.makedirs(self.output_reduce_directory)

        reduced_dict = f"{self.output_reduce_directory}/reduced_dict.txt"
        reduce_write = open(reduced_dict, "w")

        self.reduce(temp_map, reduce_write)
