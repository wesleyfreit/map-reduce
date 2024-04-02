import os
import random
import threading


class FileGenerator:
    def __init__(
        self,
        split: int,
        n: int,
        alphabet: list[str],
        min_size: int,
        max_size: int,
    ):
        self.split = split
        self.n = n
        self.alphabet = alphabet
        self.min_size = min_size
        self.max_size = max_size
        self.input_directory = "./out/dict"
        self.output_directory = "./out/files"

    def generate_random_word(self):
        length = random.randint(self.min_size, self.max_size)
        return "".join(random.choice(self.alphabet) for _ in range(length))

    def generate_dict(self):
        with open(f"{self.input_directory}/dict.txt", "w") as f:
            for _ in range(self.n):
                word = self.generate_random_word()
                f.write(f"{word} ")

    def divide_dict(self, i, words_per_file):
        with open(f"{self.input_directory}/dict.txt", "r") as dict_file:
            words = dict_file.read().split()

        start = i * words_per_file
        end = start + words_per_file
        words_to_write = words[start:end]

        words_per_line = 10

        with open(f"{self.output_directory}/file{i+1}.txt", "w") as f:
            for i in range(0, len(words_to_write), words_per_line):
                line = ' '.join(words_to_write[i:i+words_per_line])
                f.write(f"{line}\n")

    def rm_files(self):
        for file in os.listdir(self.output_directory):
            os.remove(f"{self.output_directory}/{file}")

    def execute(self):
        if not os.path.exists(self.input_directory):
            os.makedirs(self.input_directory)

        if not os.path.exists(self.output_directory):
            os.makedirs(self.output_directory)

        self.rm_files()

        self.generate_dict()

        threads: list[threading.Thread] = []
        words_per_file = self.n // self.split
        extra_words = self.n % self.split

        for i in range(self.split):
            words_in_this_file = words_per_file + (1 if i < extra_words else 0)
            thread = threading.Thread(
                target=self.divide_dict, args=(i, words_in_this_file)
            )

            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
