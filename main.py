import re
import pymp
import time

FILE_KEYS = ('shakespeare1.txt', 'shakespeare2.txt', 'shakespeare3.txt',
             'shakespeare4.txt', 'shakespeare5.txt', 'shakespeare6.txt',
             'shakespeare7.txt', 'shakespeare8.txt')

WORD_KEYS = ('hate', 'love', 'death', 'night', 'sleep', 'time', 'henry', 'hamlet',
             'you', 'my', 'blood', 'poison', 'macbeth', 'king', 'heart', 'honest')


def map_reduce(num_threads):
    # Global dictionary
    word_dict = pymp.shared.dict()
    for word_key in WORD_KEYS:
        word_dict[word_key] = 0

    print("~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>")
    print("Starting map reduce with {} thread(s)".format(num_threads))
    init_time = time.perf_counter()

    with pymp.Parallel(num_threads) as p:
        file_word_dict = dict()
        for word_key in WORD_KEYS:
            file_word_dict[word_key] = 0

        for file_key in p.range(len(FILE_KEYS)):
            for word_key in WORD_KEYS:

                count = 0
                with open(FILE_KEYS[file_key], 'r') as f:
                    for line in f:
                        count += len(re.findall(word_key, line, re.IGNORECASE))
                file_word_dict[word_key] += count

        lock = p.lock
        for word_key in WORD_KEYS:
            lock.acquire()
            word_dict[word_key] += file_word_dict[word_key]
            lock.release()

    end_time = time.perf_counter()
    print("Total word count: {}, individual counts:".format(sum(word_dict.values())))
    print(word_dict)
    print("Time with {} thread(s): {} s".format(num_threads, end_time - init_time))
    print("~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>")


for i in range(4):
    map_reduce(2 ** i)
