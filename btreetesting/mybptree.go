package btreetesting

import (
	"encoding/binary"
	"errors"
)

const (
	BPTREE_ORDER            = 256
	MAX_KEYS_PER_NODE       = 2 * BPTREE_ORDER
	MAX_VALUES_PER_LEAF     = MAX_KEYS_PER_NODE
	MAX_CHILDREN_PER_BRANCH = MAX_KEYS_PER_NODE + 1
	KEY_SIZE                = 4
	VALUE_SIZE              = 4
	// PAGESIZE                = 4096
)

type BPTree struct {
	root Node
}

func NewBPTree() *BPTree {
	return &BPTree{root: &LeafNode{}}
}

// given a branch node, returns the child node that should be traversed to to find the key
func (n *BranchNode) traverse_toward(key uint32) Node {
	for i := range n.keys {
		if i == n.num_keys {
			return n.children[i]
		}
		if key < n.keys[i] {
			return n.children[i]
		}
	}
	panic("unreachable")
}

// find the leaf node that should contain the key if the key exists.
//
// Note that this function does not guarantee the key is actually present in the
// returned leaf node
func (b *BPTree) findLeafNode(key uint32) *LeafNode {
	curr_node := b.root
	for !curr_node.isLeaf() {
		curr_node = (curr_node).(*BranchNode).traverse_toward(key)
	}
	return curr_node.(*LeafNode)
}

// returns the value associated for key, 0 if not found, all values should be >0 and keys must be >0
func (b *BPTree) findKV(key uint32) uint32 {
	leaf_node := b.findLeafNode(key)
	val, present := leaf_node.get(key)
	if !present {
		return 0
	}
	return val
}

// returns leaf node associated with this key or nil if failed
func (b *BPTree) findNode(key uint32) leafNode3232 {
	leaf_node := b.findLeafNode(key)
	_, present := leaf_node.get(key)
	if !present {
		return nil
	}
	return leaf_node
}

// returns the first leaf node in the bptree or nil if failed
func (b *BPTree) firstLeafNode() leafNode3232 {
	curr_node := b.root
	for !curr_node.isLeaf() {
		curr_node = (curr_node).(*BranchNode).children[0]
	}
	return curr_node.(*LeafNode)
}

// errors when key already exists or some tree operation failed
func (b *BPTree) insertNodeKV(key uint32, value uint32) error {
	leaf_node := b.findLeafNode(key)
	return leaf_node.insert(key, value, b)
}

func (l *LeafNode) insert(key uint32, value uint32, tree *BPTree) error {
	err := l.naive_insert(key, value)
	if err != nil {
		return err
	}
	if l.num_keys <= MAX_KEYS_PER_NODE {
		return nil
	}

	// else there are 513 keys, need to split

	// split the node
	curr := l
	new := &LeafNode{}

	// keep 257 keys
	if MAX_KEYS_PER_NODE%2 != 0 {
		curr.num_keys = (MAX_KEYS_PER_NODE / 2) + 1
	} else {
		// as currently implemented, this will never be hit - but it's here for future-proofing
		curr.num_keys = MAX_KEYS_PER_NODE / 2
	}
	// new gets 256 keys
	new.num_keys = MAX_KEYS_PER_NODE / 2

	for i := 0; i < new.num_keys; i++ {
		new.keys_arr[i] = curr.keys_arr[curr.num_keys+i]
		new.values_arr[i] = curr.values_arr[curr.num_keys+i]
	}

	new.next_leaf = curr.next_leaf
	curr.next_leaf = new

	if curr.parent != nil {
		new.parent = curr.parent

		err := curr.parent.insert(new.keys_arr[0], Node(new), tree)
		if err != nil {
			return err
		}
	} else {
		// we're splitting the root
		tree.split_root(Node(curr), Node(new))
	}

	return nil
}

func (l *LeafNode) naive_insert(key uint32, value uint32) error {
	if l.num_keys > MAX_KEYS_PER_NODE {
		panic("illegal state: leaf node has too many keys")
	}

	// TODO: bounds check elimination candidate?
	index := 0
	for index < l.num_keys && key > l.keys_arr[index] {
		index++
	}

	if key == l.keys_arr[index] {
		return errors.New("key already exists")
	}

	for i := l.num_keys; i > index; i-- {
		l.keys_arr[i] = l.keys_arr[i-1]
		l.values_arr[i] = l.values_arr[i-1]
	}

	l.keys_arr[index] = key
	l.values_arr[index] = value
	l.num_keys++

	return nil
}

func (b *BranchNode) insert(key uint32, child Node, tree *BPTree) error {
	err := b.naive_insert(key, child)
	if err != nil {
		return err
	}
	if b.num_keys <= MAX_KEYS_PER_NODE {
		return nil
	}

	// split the node
	curr := b
	new := &BranchNode{}

	// keep 257 keys
	if MAX_KEYS_PER_NODE%2 != 0 {
		curr.num_keys = (MAX_KEYS_PER_NODE / 2) + 1
	} else {
		// as currently implemented, this will never be hit - but it's here for future-proofing
		curr.num_keys = MAX_KEYS_PER_NODE / 2
	}
	// new gets 256 keys
	new.num_keys = MAX_KEYS_PER_NODE / 2

	for i := 0; i < new.num_keys; i++ {
		new.keys[i] = curr.keys[curr.num_keys+i]
		new.children[i] = curr.children[curr.num_keys+i]
	}
	new.children[new.num_keys] = curr.children[MAX_CHILDREN_PER_BRANCH-1]

	if b.parent != nil {
		new.parent = b.parent

		err := b.parent.insert(new.keys[0], Node(new), tree)
		if err != nil {
			return err
		}
	} else {
		tree.split_root(Node(curr), Node(new))
	}

	return nil
}

func (b *BPTree) split_root(left_child Node, right_child Node) {
	new_root := &BranchNode{}

	new_root.num_keys = 1
	if right_child.isLeaf() {
		new_root.keys[0] = right_child.(*LeafNode).keys_arr[0]
	} else {
		new_root.keys[0] = right_child.(*BranchNode).keys[0]
	}

	new_root.children[0] = left_child
	new_root.children[1] = right_child

	left_child.setParent(new_root)
	right_child.setParent(new_root)

	b.root = new_root
}

func (b *BranchNode) naive_insert(key uint32, child Node) error {
	if b.num_keys > MAX_KEYS_PER_NODE {
		panic("illegal state: branch node has too many keys")
	}

	// TODO: bounds check elimination candidate?
	index := 0
	for index < b.num_keys && key > b.keys[index] {
		index++
	}

	if key == b.keys[index] {
		return errors.New("key already exists")
	}

	for i := b.num_keys; i > index; i-- {
		b.keys[i] = b.keys[i-1]
		b.children[i+1] = b.children[i]
	}

	b.keys[index] = key
	b.children[index+1] = child
	b.num_keys++

	return nil
}

// errors when tree fails or when key does not exist
func (b *BPTree) overrideNodeKV(key uint32, value uint32) error {
	leaf_node := b.findLeafNode(key)

	for i := range leaf_node.keys_arr {
		if i == leaf_node.num_keys || leaf_node.keys_arr[i] > key {
			break
		}
		if leaf_node.keys_arr[i] == key {
			leaf_node.values_arr[i] = value
			return nil
		}
	}
	return errors.New("key does not exist")
}

type Node interface {
	isLeaf() bool
	size() int
	setParent(*BranchNode)
	// TODO
	addValue(int)
}

// there's an extra slot for insertion before splitting. As soon as the node is
// full it should be split.
type BranchNode struct {
	parent   *BranchNode
	keys     [MAX_KEYS_PER_NODE + 1]uint32
	children [MAX_CHILDREN_PER_BRANCH + 1]Node
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

func (b *BranchNode) addValue(val int) {
	panic("unimplemented")
}

// there's an extra slot for insertion before splitting. As soon as the node is
// full it should be split.
type LeafNode struct {
	parent     *BranchNode
	keys_arr   [MAX_KEYS_PER_NODE + 1]uint32
	values_arr [MAX_VALUES_PER_LEAF + 1]uint32
	next_leaf  *LeafNode
	num_keys   int
}

// keys implements leafNode3232.
func (l *LeafNode) keys() []uint32 {
	return l.keys_arr[:l.num_keys]
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

func (l *LeafNode) addValue(val int) {
	panic("unimplemented")
}

func (l *LeafNode) toBytes() []byte {
	var buf [PAGESIZE]byte

	for i := 0; i < l.num_keys; i++ {
		binary.LittleEndian.PutUint32(buf[4*i:], l.keys_arr[i])
		binary.LittleEndian.PutUint32(buf[4*i+PAGESIZE/2:], l.values_arr[i])
	}

	return buf[:]
}

func (l *LeafNode) fromBytes(bytes []byte) error {
	if len(bytes) != PAGESIZE {
		return errors.New("invalid byte length")
	}

	i := 0
	slice := bytes[:PAGESIZE/2]
	for i = range slice {
		index := i * 4
		key := binary.LittleEndian.Uint32(slice[index:])
		if key == 0x00_00_00_00 {
			break
		}
		value := binary.LittleEndian.Uint32(bytes[PAGESIZE/2+index:])
		l.keys_arr[i] = key
		l.values_arr[i] = value
	}
	l.num_keys = i

	return nil
}

func (l *LeafNode) nextLeaf() leafNode3232 {
	return l.next_leaf
}

// get returns the value associated with key, 0 if not found
func (l *LeafNode) get(key uint32) (uint32, bool) {
	if key < l.keys_arr[0] {
		return 0, false
	}

	for i := range l.keys_arr {
		if i == l.num_keys {
			break
		}
		if l.keys_arr[i] == key {
			return l.values_arr[i], true
		}
	}
	return 0, false
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
