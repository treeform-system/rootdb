package btreetesting

const (
	BPTREE_ORDER = 256
)

// write your struct and functions below here for example:
type BPTree struct {
	root *Node
}

// findKV implements BPTree3232.
func (b *BPTree) findKV(key uint32) uint32 {
	return findKV(key, b.root)
}

func findKV(key uint32, curr_node *Node) uint32 {
	panic("unimplemented")

	if (*curr_node).isLeaf() {
		// search for key in leaf node
		// if found, return value
		// else return 0
	} else {
		// iterate [0, num_keys) in branch node
		// if key < keys[i], recurse on children[i]
	}
}

// findNode implements BPTree3232.
func (b *BPTree) findNode(key uint32) leafNode3232 {
	panic("unimplemented")
}

// firstLeafNode implements BPTree3232.
func (b *BPTree) firstLeafNode() leafNode3232 {
	panic("unimplemented")
}

// insertNodeKV implements BPTree3232.
func (b *BPTree) insertNodeKV(key uint32, value uint32) error {
	panic("unimplemented")
}

// overrideNodeKV implements BPTree3232.
func (b *BPTree) overrideNodeKV(key uint32, value uint32) error {
	panic("unimplemented")
}

type Node interface {
	isLeaf() bool
	size() int
	setParent(*BranchNode)
	// TODO
	addValue(int)
}

type BranchNode struct {
	parent   *BranchNode
	keys     [BPTREE_ORDER*2 + 1]uint32
	children [BPTREE_ORDER*2 + 2]*Node
	num_keys int
}

func (b *BranchNode) isLeaf() bool {
	return false
}

func (b *BranchNode) size() int {
	return b.num_keys
}

func (b *BranchNode) setParent(p *BranchNode) {
	b.parent = p
}

type LeafNode struct {
	parent     *BranchNode
	keys_arr   [BPTREE_ORDER*2 + 1]uint32
	values_arr [BPTREE_ORDER*2 + 1]uint32
	next_leaf  *LeafNode
	num_keys   int
}

// keys implements leafNode3232.
func (l *LeafNode) keys() []uint32 {
	return l.keys_arr[:l.num_keys]
}

func get_val(l *LeafNode, key uint32) (uint32, bool) {
	for i := 0; i < l.num_keys; i++ {
		if l.keys_arr[i] == key {
			return l.keys_arr[i], true
		}
	}
	return 0, false
}

// values implements leafNode3232.
func (l *LeafNode) values() []uint32 {
	return l.values_arr[:l.num_keys]
}

func (l *LeafNode) isLeaf() bool {
	return true
}

func (l *LeafNode) size() int {
	return l.num_keys
}

func (l *LeafNode) setParent(p *BranchNode) {
	l.parent = p
}

func (l *LeafNode) toBytes() []byte {
	panic("unimplemented")
}

func (l *LeafNode) fromBytes(bytes []byte) error {
	panic("unimplemented")
}

func (l *LeafNode) nextLeaf() leafNode3232 {
	return l.next_leaf
}

// Interfaces below DO NOT MODIFY
//
// This tree has uint32:uint32 key-value pairs
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

// This tree has int64:uint32 key-value pairs
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

// This tree has int64 unique value keys
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
