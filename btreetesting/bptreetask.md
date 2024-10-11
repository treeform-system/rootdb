# Task

- B+Tree rewrite

## Goal
rewrite B+Tree to support different units. Current implementation has a **int64:int64** key-value pair. Choose to rewrite into one of the following formats: **uint32:uint32** key-value, **int64:uint32** keyvalue, or **int64** unique set.

## Current system

Within the btreetesting folder there is a mybptree.go file and a mybptree_test.go file, these are the files you will be changing. The current bptree in the internals folder saves the bptree into a file, it is not necessary to copy this implementation. For this task it is enough to rewrite to contain the bptree in memory only (if you want you can have it persistent to file). The leafbuffer struct in the bptree is responsible for all file writes and therefore you can ignore any of the operations performed by the leafbuffer and unnecessary to even use this struct for your tree.

In golang interfaces are implicitly defined on struct's, therefore all you need to do is ensure your struct has all the functions within the interface and the struct will be assignable to the interface. This is used in testing, within the test file you will find a region that says initialize tree, here you can write whatever initialization you want or not want for your tree and the rest of the test uses the interface to operate on it. Here are the interfaces for the key value trees:
```go
type BPTree3232 interface {
	insertNodeKV(key, value uint32) error   //errors when key already exists or some tree operation failed
	overrideNodeKV(key, value uint32) error //errors when tree fails or when key does not exist
	findKV(key uint32) uint32               //returns the value associated for key, 0 if not found, all values should be >0 and keys must be >0
	findNode(key uint32) leafNode3232       //returns leaf node associated with this key or nil if failed
	firstLeafNode() leafNode3232            //returns the first leaf node in the bptree or nil if failed
}

type BPTree6432 interface {
	insertNodeKV(key int64, value uint32) error   //errors when key already exists or some tree operation failed
	overrideNodeKV(key int64, value uint32) error //errors when tree fails or when key does not exist
	findKV(key int64) uint32                      //returns the value associated for key, 0 if not found, all values should be >0 and keys must be >0
	findNode(key int64) leafNode6432              //returns leaf node associated with this key or nil if failed
	firstLeafNode() leafNode6432                  //returns the first leaf node in the bptree or nil if failed
}

type leafNode interface{
	toBytes() []byte
	fromBytes(bytes []byte) error
	nextLeaf() leafNode
	keys() [][int32|int64]
	values() [][int32|int64]
}
```

I've only specified a leaf node interface you will probably want a seperate struct for the branch nodes though. These interfaces apply to the int64/int32 key-value I will specify the functions for the unique bptree further down. 

The functions are mostly self-explanatory, but for a general purpose you want to be able to insert key-value pairs without collisions, override previous key-value pairs, select the value from a specific key, return the leafnode from a specific key being stored, and a function to return the leftmost leafnode otherwise known as the start node. The leafNode must be able to be converted to a 4096 length byte slice and be converted back to a leafnode from this byte slice to retain its previous state.

### Unique BPTree

```go
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
```
For the unique tree instead of having a key-value pair you only have a key and the purpose is to check if a key exists or not. This is very similar to a hashmap/set but instead of being O(1) for finding a key it is O(logn) for finding a key. This seems worse but the tradeoffs on large datasets actually make this a much better option due to not requiring the full rewrites when a hashmap overflows and in a database you can have very large amounts of data and that's why we're using a BPTree instead of a hashmap.

## Testing

In golang you can simply run a test file by entering the directory where the test file is stored and it run `go test` this will test all functions but since you are only writing one type of bptree you can use `go test -run <functionaName>` for whichever function you need for your type of tree (ie. `go test -run TestBPTree6432`). If it is all correct it will simply say "ok" otherwise it will tell you where it failed you can add debug logs where needed but do not remove any tests or insert new operations into the middle of the test. The test are meant to represent how this tree will be used in the overall system. The only section you modify is the initialization area for your tree.

## Getting started

Make your own branch (ie. git checkout -b "mybranch" or google to see actual syntax) to work on your stuff without interfering with other ppl stuff. No need to merge back to experimental, also moving forward you shouldn't touch the main branch and no one should have permission either unless I messed up.

Going forward this will be the main template for the task you pick up(in this case everyone doing the same thing mostly), I will present a problem in this case adding different units to the bptree, and your goal is to make sure the tests pass. It is up to you to do this however you want in this case since it is the first task and I have already made an implementation there is a file in the internals folder called "bptree.go" this is a working implementation that is persistent you can ignore the leafbuffer parts but you can start by copying the functions and seeing which parts you actually need to change to accomplish what you want. I suggest you define the struct for your tree carefully. You are not limited by the functions in the interface they are a minimum but you can define whatever you want. Do not change the interfaces though. Good luck, the goal is to get the test to pass for your tree if you have a problem attempt something and have some program that compiles then message me and we can find a time to step through the program and find the logic flaw. (You can ignore the other files in this folder)

there is also a function called "TestBPTreeDebugging" in the test file you can use for some debugging or intermediate testing. `go test - run TestBPTreeDebugging` you must use this in a terminal to get log outputs, if you run this directly from vscode it will send logs to void but error logs or fatal logs get shown either way. 