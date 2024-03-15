from file_generator import FileGenerator
from map_reduce import MapReduce


file_gen = FileGenerator(
    4,
    10,
    list(["a", "e", "o", "s", "r", "i", "n", "d", "m", "u"]),
    3,
    5,
)

map_reduce = MapReduce()

file_gen.execute()
map_reduce.execute()
