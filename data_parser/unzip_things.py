from bz2 import BZ2File as bzopen

all_filenames = !find data -name "*.bz2"

with open('mega_file.json', 'wb') as f:
    for file in all_filenames:
        stream = bzopen(file, 'r')
        for line in stream:
            f.write(line)
        stream.close()


