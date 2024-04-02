from io import TextIOWrapper
import os
import re
import threading


class MapReduce:

    def __init__(self, pattern: str):
        self.pattern = pattern
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

    def map_grep(self, file: str, filename: str, map_write: TextIOWrapper):
        with open(file, "r") as f:
            for line in f:
                if self.pattern != "":
                    if re.search(self.pattern, line):
                        map_write.write(f"{filename} | {line.strip()}\n")
                else:
                    map_write.write(f"{filename} | {line.strip()}\n")

    def reduce_grep(self, temp_map: str, reduce_write: TextIOWrapper):
        lines = []

        with open(temp_map, "r") as f:
            lines = f.readlines()

        lines.sort()

        reduce_write.write("".join(lines))

    def execute(self):
        if not os.path.exists(self.output_map_directory):
            os.makedirs(self.output_map_directory)

        threads: list[threading.Thread] = []

        temp_map_default = f"{self.output_map_directory}/temp_map_default.txt"
        temp_map_grep = f"{self.output_map_directory}/temp_map_grep.txt"

        if os.path.exists(temp_map_default):
            os.remove(temp_map_default)

        if os.path.exists(temp_map_grep):
            os.remove(temp_map_grep)

        map_default_write = open(temp_map_default, "a")
        map_grep_write = open(temp_map_grep, "a")

        for file in os.listdir(self.input_directory):
            if file.endswith(".txt"):
                filename = file

                file = f"{self.input_directory}/{file}"

                thread_default = threading.Thread(
                    target=self.map, args=(file, map_default_write)
                )

                thread_grep = threading.Thread(
                    target=self.map_grep, args=(file, filename, map_grep_write)
                )

                threads.append(thread_default)
                threads.append(thread_grep)

                thread_default.start()
                thread_grep.start()

        for thread in threads:
            thread.join()

        map_default_write.close()
        map_grep_write.close()

        if not os.path.exists(self.output_reduce_directory):
            os.makedirs(self.output_reduce_directory)

        reduced_dict_default = (
            f"{self.output_reduce_directory}/reduced_dict_default.txt"
        )
        reduced_dict_grep = (
            f"{self.output_reduce_directory}/reduced_dict_grep.txt"
        )

        reduce_default_write = open(reduced_dict_default, "w")
        reduce_grep_write = open(reduced_dict_grep, "w")

        self.reduce(temp_map_default, reduce_default_write)
        self.reduce_grep(temp_map_grep, reduce_grep_write)
