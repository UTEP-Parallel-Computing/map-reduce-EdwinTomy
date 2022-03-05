import time
import re
from mpi4py import MPI

# MPI parameters
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Files names
FILE_KEYS = ('shakespeare1.txt', 'shakespeare2.txt', 'shakespeare3.txt',
             'shakespeare4.txt', 'shakespeare5.txt', 'shakespeare6.txt',
             'shakespeare7.txt', 'shakespeare8.txt')

# Words to be counted
WORD_KEYS = ('hate', 'love', 'death', 'night', 'sleep', 'time', 'henry', 'hamlet',
             'you', 'my', 'blood', 'poison', 'macbeth', 'king', 'heart', 'honest')

word_cnt = {}
for word_key in WORD_KEYS:
    word_cnt[word_key] = 0

# Distribution of work
if rank is 0:

    print(f'Running program with {size} threads.')
    INIT_TIME = time.clock_gettime(time.CLOCK_MONOTONIC_RAW)
    local_file_cnt = len(FILE_KEYS) / size

    # Base case of thread 0
    thread_file_list = FILE_KEYS[:int(local_file_cnt)]
    file_content = []
    for file in thread_file_list:
        with open(file, encoding='UTF-8') as f:
            f.seek(0)
            string = f.read()
            file_content.append(string.lower())
            f.close()
    for file in file_content:
        for word in WORD_KEYS:
            length = len(re.findall(word, file))
            word_cnt[word] = word_cnt[word] + length

    print(f'Thread #{rank} has individual word count: \n{word_cnt}')

    # Distribute work
    for process in range(1, size):
        start_idx = int(local_file_cnt * process)
        end_idx = int(local_file_cnt * (process + 1))
        thread_file_list = FILE_KEYS[start_idx:end_idx]
        comm.send(thread_file_list, dest=process)

        # Individual counts
        thread_word_cnt = comm.recv(source=process)
        for key in word_cnt:
            if key in thread_word_cnt:
                word_cnt[key] = word_cnt[key] + thread_word_cnt[key]

    print(f'Final word count:')
    print(word_cnt)
    END_TIME = time.clock_gettime(time.CLOCK_MONOTONIC_RAW) - INIT_TIME
    print(f'Total time: {END_TIME} seconds!')


# Other parallel programs apart from 0
else:
    thread_file_list = comm.recv(source=0)

    # Reading per line
    file_content = []
    for file in thread_file_list:
        with open(file, encoding='UTF-8') as f:
            f.seek(0)
            string = f.read()
            file_content.append(string.lower())
            f.close()

    #
    for file in file_content:
        for word in WORD_KEYS:
            length = len(re.findall(word, file))
            word_cnt[word] = word_cnt[word] + length

    print("~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>")
    print(f'Thread #{rank} has individual word count: \n{word_cnt}')
    print("~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>~>")
    comm.send(word_cnt, dest=0)
