from file_generator import FileGenerator
from map_reduce import MapReduce


if __name__ == "__main__":
    split = int(input("Insira o número de arquivos a serem gerados: "))
    n = int(input("Insira o número de palavras a serem geradas: "))
    alphabet = list(input("Insira o alfabeto a ser utilizado: "))
    min_size = int(input("Insira o tamanho mínimo das palavras: "))
    max_size = int(input("Insira o tamanho máximo das palavras: "))

    file_gen = FileGenerator(split, n, alphabet, min_size, max_size)

    file_gen.execute()

    pattern = input("Insira uma expressão regular: ")

    map_reduce = MapReduce(pattern)
    map_reduce.execute()
