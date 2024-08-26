# Page specification layout

- bytes 0-8 = page number
- bytes 8-10 = number of rows
- bytes 10-26 = checksum
- bytes 26-PAGESIZE = data

limited writer:
    writer fetches page, copies buffer -> writes new page held in memory till transaction done
    then swaps page to actual file and writes page, new data would be reading from original data on file
    lock would lock any two transactions trying to physically write page to disk
    (https://cs186berkeley.net/notes/note11/)