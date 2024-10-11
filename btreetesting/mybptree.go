package btreetesting

// write your struct and functions below here for example:
type myTree struct {
	keys   [256]int64
	values [256]int64
}

//
// Interfaces below DO NOT MODIFY
//
// This tree has uint32:uint32 key-value pairs
//
type BPTree3232 interface {
	insertNodeKV(key, value uint32) error   //errors when key already exists or some tree operation failed
	overrideNodeKV(key, value uint32) error //errors when tree fails or when key does not exist
	findKV(key uint32) uint32               //returns the value associated for key, 0 if not found, all values should be >0 and keys must be >0
	findNode(key uint32) leafNode3232       //returns leaf node associated with this key or nil if failed
	firstLeafNode() leafNode3232            //returns the first leaf node in the bptree or nil if failed
}

type leafNode3232 interface {
	toBytes() []byte
	fromBytes(bytes []byte) error
	nextLeaf() leafNode3232
	keys() []uint32
	values() []uint32
}

//
// This tree has int64:uint32 key-value pairs
//
type BPTree6432 interface {
	insertNodeKV(key int64, value uint32) error   //errors when key already exists or some tree operation failed
	overrideNodeKV(key int64, value uint32) error //errors when tree fails or when key does not exist
	findKV(key int64) uint32                      //returns the value associated for key, 0 if not found, all values should be >0 and keys must be >0
	findNode(key int64) leafNode6432              //returns leaf node associated with this key or nil if failed
	firstLeafNode() leafNode6432                  //returns the first leaf node in the bptree or nil if failed
}

type leafNode6432 interface {
	toBytes() []byte
	fromBytes(bytes []byte) error
	nextLeaf() leafNode6432
	keys() []int64
	values() []uint32
}

//
// This tree has int64 unique value keys
//
type BPTreeUnique interface {
	insertKey(key int64) error         //some tree error or key already exists
	keyExists(key int64) bool          //returns true if key in set
	findNode(key int64) leafNodeUnique //returns leaf node associated with this key or nil if failed
	firstLeafNode() leafNodeUnique     //returns the first leaf node in the bptree or nil if failed
}
type leafNodeUnique interface {
	toBytes() []byte
	fromBytes(bytes []byte) error
	nextLeaf() leafNodeUnique
	keys() []int64
}
