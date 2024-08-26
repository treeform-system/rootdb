# LeafBuffer

new nodes are written sequentially in file but in meta page correct array of order of those pages in btree leaves are put and when pages are read sequentially in initial the array of int16 is used to order the pages and then combine them afterwards sorted by the meta array

### Meta Page format for index file

currently unnecessary and merely used as offset for future compatibilty
all leaf pages are composed of array of key and value int64