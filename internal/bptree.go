package internal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	ORDER  = 128 //order*2=256 which is how many fit in a PAGESIZE block 4096/16(16 = 2 int64)
	MAXAMT = ORDER*2 + 1
)

type tree struct {
	root    node
	leafBuf *leafBuffer
}

// written PAGESIZE+PAGESIZE*meta_pagenum
type node interface {
	isLeaf() bool
	size() int //represent how many values in node
	addValue(int)
	setParent(node)
}

/*
Creates file if not exist, otherwise opens to read
File combination of the directory and concatenation of "id"+"<tablename>"+"<columname>"
returns valid tree with at least one node
Tree contains close function must be called at end to sync
error if any part of tree doesn't work
*/
func initializeTree(dir, tablename, columname string) (*tree, error) {
	filename := filepath.Join(dir, "id"+tablename+columname+".db")
	if f, err := os.OpenFile(filename, os.O_WRONLY, 0666); err != nil {
		f, err = os.Create(filename)
		if err != nil {
			return nil, err
		}
		var newpage [PAGESIZE * 2]byte
		f.Write(newpage[:])
		f.Close()
	} else {
		f.Close()
	}

	lb := new(leafBuffer)
	root, err := lb.InitializeLeafBuffer(filename)
	if err != nil {
		return nil, err
	}

	return &tree{root: root, leafBuf: lb}, nil
}

func (t *tree) closeIndex() {
	t.leafBuf.closeLeafBuffer()
}

func (t *tree) findKeyValue(key int64) (int64, bool) {
	n := t.findLeaf(t.root, key)
	return t.findValue(n, key)
}

func (t *tree) findLeaf(thisNode node, key int64) *leafint64Node {
	if thisNode == nil {
		return nil
	}
	if thisNode.isLeaf() {
		return thisNode.(*leafint64Node)
	}
	//otherwise its a branch
	branchnode := thisNode.(*branchNode)
	index := 0
	for i := range branchnode.keys {
		if key < branchnode.keys[i] || branchnode.keys[i] == 0 { //i >= (ORDER*2+1-thisNode.nums) (this is wrong meaning at some point i messed up setting the nums field in a node)
			return t.findLeaf(branchnode.pointers[index], key)
		}
		index++
	}
	return t.findLeaf(branchnode.pointers[index], key)
}

func (t *tree) findValue(leafnode node, key int64) (int64, bool) {
	if leafnode == nil || !leafnode.isLeaf() {
		return 0, false
	}
	thisLeaf := leafnode.(*leafint64Node)
	for i := range thisLeaf.keys {
		if key == thisLeaf.keys[i] {
			return thisLeaf.values[i], true
		}
	}
	return 0, false
}

func (t *tree) insertNode(key int64, value int64) error {
	leaf := t.findLeaf(t.root, key)

	if leaf == nil {
		return errors.New("unable to insert node")
	}
	if _, ok := t.findValue(leaf, key); ok {
		return errors.New("key already exists")
	}
	if leaf.size() == MAXAMT-1 {
		//split node into two
		anotherLeaf := new(leafint64Node) //new one
		thisLeaf := leaf

		InsertKVLeafNode(leaf, key, value)

		count := 0
		for i := ORDER; i < ORDER*2+1; i++ { //modify to change distribution on split
			anotherLeaf.keys[i-ORDER] = thisLeaf.keys[i]
			anotherLeaf.values[i-ORDER] = thisLeaf.values[i]
			thisLeaf.keys[i] = 0
			thisLeaf.values[i] = 0
			count++
		}

		anotherLeaf.next = thisLeaf.next
		thisLeaf.next = anotherLeaf
		thisLeaf.addValue(-count)
		anotherLeaf.addValue(count)
		anotherLeaf.parent = thisLeaf.parent
		t.leafBuf.lastPageId++
		anotherLeaf.meta_pageNum = t.leafBuf.lastPageId
		t.leafBuf.writeNewPage(thisLeaf, anotherLeaf)
		t.propogateBranchKeyUp(thisLeaf.parent, anotherLeaf.keys[0], leaf, anotherLeaf) //propogate key upwards
	} else {
		InsertKVLeafNode(leaf, key, value)
		t.leafBuf.writeLeafToBuffer(leaf)
	}

	return nil
}

func (t *tree) propogateBranchKeyUp(thisParent node, key int64, left, right node) {
	if thisParent == nil {
		topBranch := new(branchNode)
		topBranch.keys[0] = key
		topBranch.pointers[0] = left
		topBranch.pointers[1] = right
		topBranch.nums = 1
		t.root = topBranch
		left.setParent(t.root)
		right.setParent(t.root)
		return
	}

	if thisParent.size() == MAXAMT-1 {
		anotherBranch := new(branchNode)

		InsertBranchKCNode(thisParent, key, left, right)

		parentBranch := thisParent.(*branchNode)
		middleKey := parentBranch.keys[ORDER]

		for i := ORDER + 1; i < ORDER*2+1; i++ {
			anotherBranch.keys[i-ORDER-1] = parentBranch.keys[i]
			parentBranch.keys[i] = 0
		}
		for i := ORDER + 1; i < ORDER*2+2; i++ {
			anotherBranch.pointers[i-ORDER-1] = parentBranch.pointers[i]
			parentBranch.pointers[i].setParent(anotherBranch)
			parentBranch.pointers[i] = nil
		}
		anotherBranch.parent = parentBranch.parent
		anotherBranch.nums = ORDER
		parentBranch.nums = ORDER

		t.propogateBranchKeyUp(parentBranch.parent, middleKey, parentBranch, anotherBranch)
	} else {
		InsertBranchKCNode(thisParent, key, left, right)
	}
}

func (t *tree) findFirstLeaf() *leafint64Node {
	currentLeaf := t.root
	for currentLeaf != nil && !currentLeaf.isLeaf() {
		currentBranch := currentLeaf.(*branchNode)
		currentLeaf = currentBranch.pointers[0]
	}
	if currentLeaf == nil {
		return nil
	}
	return currentLeaf.(*leafint64Node)
}

type branchNode struct {
	parent   node
	keys     [ORDER*2 + 1]int64
	pointers [ORDER*2 + 2]node
	nums     int //less than max keys
}

func (b *branchNode) isLeaf() bool {
	return false
}

func (b *branchNode) size() int {
	return b.nums
}

func (b *branchNode) addValue(delta int) {
	b.nums += delta
}

func (b *branchNode) setParent(parent node) {
	b.parent = parent
}

func (b *branchNode) String() string {
	return fmt.Sprintf("(%d)", b.nums)
}

type leafint64Node struct {
	nums         int                //less than max keys
	keys         [ORDER*2 + 1]int64 //zero represents unset key
	values       [ORDER*2 + 1]int64
	next         *leafint64Node
	parent       node
	meta_pageNum uint32
	meta_min     int64 //same type as values
}

func (l *leafint64Node) isLeaf() bool {
	return true
}

func (l *leafint64Node) size() int {
	return l.nums
}

func (l *leafint64Node) addValue(delta int) {
	l.nums += delta
}

func (l *leafint64Node) setParent(parent node) {
	l.parent = parent
}

func (l *leafint64Node) String() string {
	return fmt.Sprintf("(%d)", l.nums)
}

//
// # General Functions
//

func InsertKVLeafNode(node node, key int64, value int64) {
	leafNode, ok := node.(*leafint64Node)
	if !ok {
		panic("node interface set to leafnode untrue (InsertKVLeafNode)")
	}
	index := 0
	for i := range leafNode.keys {
		if key < leafNode.keys[i] || leafNode.keys[i] == 0 {
			break
		}
		index++
	}
	for i := ORDER * 2; i > index; i-- {
		leafNode.keys[i] = leafNode.keys[i-1]
		leafNode.values[i] = leafNode.values[i-1]
	}
	leafNode.keys[index] = key
	leafNode.values[index] = value
	leafNode.nums++
}

func InsertBranchKCNode(node node, key int64, left, right node) {
	branchNode := node.(*branchNode)
	index := 0
	for i := range branchNode.keys {
		if key < branchNode.keys[i] || branchNode.keys[i] == 0 {
			break
		}
		index++
	}
	for i := ORDER * 2; i > index; i-- {
		branchNode.keys[i] = branchNode.keys[i-1]
		branchNode.pointers[i+1] = branchNode.pointers[i]
	}

	branchNode.pointers[index] = left
	branchNode.pointers[index+1] = right
	branchNode.keys[index] = key

	branchNode.nums++
}

func bulkPropogateBranchKey(thisParent node, key int64, left, right *branchNode) {
	if thisParent == nil {
		newRoot := new(branchNode)
		newRoot.keys[0] = key
		newRoot.pointers[0] = left
		newRoot.pointers[1] = right
		newRoot.nums = 1
		left.parent = newRoot
		right.parent = newRoot
		return
	}

	if thisParent.size() == MAXAMT-1 {
		anotherBranch := new(branchNode)

		InsertBranchKCNode(thisParent, key, left, right)

		thisParent := thisParent.(*branchNode)
		middleKey := thisParent.keys[ORDER]

		for i := ORDER + 1; i < ORDER*2+1; i++ {
			anotherBranch.keys[i-ORDER-1] = thisParent.keys[i]
			thisParent.keys[i] = 0
		}
		for i := ORDER + 1; i < ORDER*2+2; i++ {
			anotherBranch.pointers[i-ORDER-1] = thisParent.pointers[i]
			thisParent.pointers[i].setParent(anotherBranch)
			thisParent.pointers[i] = nil
		}
		anotherBranch.parent = thisParent.parent
		anotherBranch.nums = ORDER
		thisParent.nums = ORDER

		bulkPropogateBranchKey(thisParent.parent, middleKey, thisParent, anotherBranch)
	} else {
		InsertBranchKCNode(thisParent, key, left, right)
	}
}

/*
uses pointer to static array since that avoids copying the array can be called either by
setting parameter to same type with pointer to it or casting parameter to slice and calling function with staticArray[:]
since also creates temp slice data type that uses pointer.
first leaf page after meta page always going to be min page
*/
type leafBuffer struct {
	indexFile     *os.File
	metapage      [PAGESIZE]byte
	lastPageId    uint32
	bufferedPages []*leafint64Node
}

func (l *leafBuffer) InitializeLeafBuffer(filepath string) (node, error) {
	var err error
	l.indexFile, err = os.OpenFile(filepath, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	l.indexFile.Read(l.metapage[:])
	l.lastPageId = 1
	l.bufferedPages = make([]*leafint64Node, 0, 5)

	listHead := l.linkLeafNodes()

	//read all pages and rerender tree from scratch

	return l.constructTree(listHead), nil
}

func (l *leafBuffer) linkLeafNodes() node {
	head := new(leafint64Node)
	tail := new(leafint64Node)
	var pageBuf [PAGESIZE]byte
	pageNum := uint32(1)

	l.indexFile.Read(pageBuf[:]) //first read always valid
	head.meta_pageNum = pageNum
	head.meta_min = MAXINT64
	var byteOffset int = 0
	for i := 0; i < ORDER*2; i++ {
		var tmpKey int64 = int64(binary.LittleEndian.Uint64(pageBuf[byteOffset:]))
		if tmpKey == 0 {
			break
		}
		if tmpKey < head.meta_min {
			head.meta_min = tmpKey
		}
		head.keys[i] = tmpKey
		byteOffset += 8
		head.values[i] = int64(binary.LittleEndian.Uint64(pageBuf[byteOffset:]))
		byteOffset += 8
		head.nums++
	}
	tail = head

	var err error
	for err == nil {
		_, err = l.indexFile.Read(pageBuf[:])
		if err != nil {
			break
		}
		newnode := new(leafint64Node)
		pageNum++
		newnode.meta_pageNum = pageNum
		newnode.meta_min = MAXINT64
		var byteOffset int = 0
		for i := 0; i < ORDER*2; i++ {
			var tmpKey int64 = int64(binary.LittleEndian.Uint64(pageBuf[byteOffset:]))
			if tmpKey == 0 {
				break
			}
			if tmpKey < newnode.meta_min {
				newnode.meta_min = tmpKey
			}
			newnode.keys[i] = tmpKey
			byteOffset += 8
			newnode.values[i] = int64(binary.LittleEndian.Uint64(pageBuf[byteOffset:]))
			byteOffset += 8
			newnode.nums++
		}
		if newnode.meta_min > tail.meta_min {
			tail.next = newnode
			tail = newnode
			continue
		}
		curr := head
		for curr != nil {
			if newnode.meta_min > curr.meta_min {
				newnode.next = curr.next
				curr.next = newnode
			}
			curr = curr.next
		}
	}

	l.lastPageId = pageNum
	return head
}

func (l *leafBuffer) constructTree(head node) *branchNode {
	thisBranch := new(branchNode)
	thisBranch.nums = 0

	var curr *leafint64Node
	thisBranch.pointers[0] = head
	head.setParent(thisBranch)
	curr = head.(*leafint64Node).next
	var index int = 0
	for curr != nil {
		if thisBranch.nums == MAXAMT-1 {
			//repeated so there is check when nums is 1 and last branch node not accidentally set to 0
			//and full and break later on for new insertion
			//since we always want to check for branch node being full since bulk loading may end with a branch being full
			//and no checks later on for fullness
			thisBranch.keys[index] = curr.meta_min
			thisBranch.pointers[index+1] = curr
			curr.parent = thisBranch
			curr = curr.next
			thisBranch.nums++

			middleKey := thisBranch.keys[ORDER]
			rightBranch := new(branchNode)

			for i := ORDER + 1; i < ORDER*2+1; i++ {
				rightBranch.keys[i-ORDER-1] = thisBranch.keys[i]
				thisBranch.keys[i] = 0
			}
			for i := ORDER + 1; i < ORDER*2+2; i++ {
				rightBranch.pointers[i-ORDER-1] = thisBranch.pointers[i]
				thisBranch.pointers[i].setParent(rightBranch)
				thisBranch.pointers[i] = nil
			}
			rightBranch.parent = thisBranch.parent
			rightBranch.nums = ORDER
			thisBranch.nums = ORDER

			bulkPropogateBranchKey(thisBranch.parent, middleKey, thisBranch, rightBranch)

			thisBranch = rightBranch
			index = 0
			continue
		}
		thisBranch.keys[index] = curr.meta_min
		thisBranch.pointers[index+1] = curr
		curr.setParent(thisBranch)
		curr = curr.next
		index++
		thisBranch.nums++
	}

	for thisBranch.parent != nil {
		thisBranch = thisBranch.parent.(*branchNode)
	}
	return thisBranch
}

func (l *leafBuffer) writeLeafToBuffer(node *leafint64Node) {
	for i := range l.bufferedPages {
		if l.bufferedPages[i].meta_min == node.meta_min {
			return
		}
	}
	l.bufferedPages = append(l.bufferedPages, node)
}

func (l *leafBuffer) removeLeafFromBuffer(leaf *leafint64Node) {
	for i := range l.bufferedPages {
		if l.bufferedPages[i].meta_pageNum == leaf.meta_pageNum {
			l.bufferedPages[i] = l.bufferedPages[len(l.bufferedPages)-1]
			l.bufferedPages = l.bufferedPages[:len(l.bufferedPages)-1]
			return
		}
	}
}

func (l *leafBuffer) writeNewPage(oldleaf, newleaf *leafint64Node) {
	l.writeLeafPage(oldleaf)
	l.writeLeafPage(newleaf)
	l.removeLeafFromBuffer(oldleaf)
}

func (l *leafBuffer) writeLeafPage(leaf *leafint64Node) {
	var offset int64 = int64(leaf.meta_pageNum) * PAGESIZE
	var leafbytes [PAGESIZE]byte
	var byteOffset int = 0
	for i := 0; i < ORDER*2; i++ {
		binary.LittleEndian.PutUint64(leafbytes[byteOffset:], uint64(leaf.keys[i]))
		byteOffset += 8
		binary.LittleEndian.PutUint64(leafbytes[byteOffset:], uint64(leaf.values[i]))
		byteOffset += 8
	}

	l.indexFile.Seek(offset, 0)
	l.indexFile.Write(leafbytes[:])
	l.removeLeafFromBuffer(leaf)
}

func (l *leafBuffer) syncNodesFromBuffer() {
	for i := range l.bufferedPages {
		l.writeLeafPage(l.bufferedPages[i])
	}
	l.bufferedPages = l.bufferedPages[:0] //sets len to 0 for reuse since underlying memory always overwritten
}

func (l *leafBuffer) closeLeafBuffer() {
	l.syncNodesFromBuffer()
	l.indexFile.Close()
}

// func main() {
// 	mytree, err := initializeTree("./", "mytable", "mycolumn")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer mytree.closeIndex()

// 	r1 := rand.New(rand.NewPCG(42, 1024))
// 	for i := int64(1); i < 200; i++ {
// 		err := mytree.insertNode(i, r1.Int64N(100))
// 		if err != nil {
// 			log.Fatal(i, err)
// 		}
// 	}
// 	err = mytree.insertNode(202, 256)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	val, ok := mytree.findKeyValue(202)
// 	fmt.Printf("%t -ok; %d - val: %d\n", ok, 202, val)

// 	val, ok = mytree.findKeyValue(201)
// 	fmt.Printf("%t -ok; %d - val: %d\n", ok, 201, val) //false

// 	err = mytree.insertNode(17, 256)
// 	if err == nil {
// 		log.Fatal("number passed through nonunique")
// 	} else {
// 		fmt.Println("correctly blocks duplicate")
// 	}

// 	for i := int64(203); i < 8000; i++ {
// 		err := mytree.insertNode(i, r1.Int64N(100))
// 		if err != nil {
// 			log.Fatal(i, err)
// 		}
// 	}

// 	val, ok = mytree.findKeyValue(7811)
// 	fmt.Printf("%t -ok; %d - val: %d\n", ok, 7811, val) //23

// 	val, ok = mytree.findKeyValue(7992)
// 	fmt.Printf("%t -ok; %d - val: %d\n", ok, 7992, val) //2

// 	val, ok = mytree.findKeyValue(7999)
// 	fmt.Printf("%t -ok; %d - val: %d\n", ok, 7999, val) //44

// 	val, ok = mytree.findKeyValue(7451)
// 	fmt.Printf("%t -ok; %d - val: %d\n", ok, 7451, val) //31

// 	val, ok = mytree.findKeyValue(7555)
// 	fmt.Printf("%t -ok; %d - val: %d\n", ok, 7555, val) //8

// 	val, ok = mytree.findKeyValue(9000)
// 	fmt.Printf("%t -ok; %d - val: %d\n", ok, 9000, val) //false

// 	val, ok = mytree.findKeyValue(1000)
// 	fmt.Printf("%t -ok; %d - val: %d\n", ok, 1000, val) //75
// }
