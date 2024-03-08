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
        self.directory = "./public"

    def generate_random_word(self):
        length = random.randint(self.min_size, self.max_size)
        return "".join(random.choice(self.alphabet) for _ in range(length))

    def generate_file(self, i, words_per_file):
        with open(f"{self.directory}/file{i+1}.txt", "w") as f:
            for _ in range(words_per_file):
                word = self.generate_random_word()
                f.write(f"{word} ")

    def generate_files(self):
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

        words_per_file = self.n // self.split
        threads: list[threading.Thread] = []

        for i in range(self.split):
            thread = threading.Thread(
                target=self.generate_file, args=(i, words_per_file)
            )

            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
