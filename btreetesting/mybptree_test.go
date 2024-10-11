package btreetesting

import (
	"errors"
	"log"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	errorBadFind = errors.New("proper value not found")
)

//there are 3 functions only need to run the specific test for your type of tree
// go test -run TestBPTree6432
// go test -run TestBPTree3232
// go test -run TestBPTreeUnique
// modify the test to put your initialization

func TestBPTreeDebugging(t *testing.T) {
	var testTree BPTree3232 = nil
	_ = testTree

	t.Log("print some debugging")
	t.Logf("formatted string %s for debugging", "for")
}

func TestBPTree6432(t *testing.T) {
	// Initialize tree here

	//EX: newtree := InitializeTree() where InitializeTree returns your intialized tree
	var testTree BPTree6432 = nil //replace nil with your newtree should compile if your tree struct properly defines the interface

	// Do not modify code below except for adding debug logs
	if testTree == nil {
		log.Fatal("bptree not initialized")
	}
	r1 := rand.New(rand.NewPCG(42, 1024))
	for i := int64(1); i < 200; i++ {
		err := testTree.insertNodeKV(i, uint32(r1.Int64N(100)))
		if err != nil {
			log.Fatal(i, err)
		}
	}
	err := testTree.insertNodeKV(202, 256)
	if err != nil {
		log.Fatal(err)
	}

	val := testTree.findKV(202)
	if val == 0 || val != 256 {
		log.Fatal(errorBadFind, 202, val)
	}

	val = testTree.findKV(201)
	if val != 0 {
		log.Fatal(errorBadFind, 201, val)
	}

	err = testTree.insertNodeKV(17, 256)
	if err == nil {
		log.Fatal("number passed through nonunique")
	}

	for i := int64(203); i < 8000; i++ {
		err := testTree.insertNodeKV(i, uint32(r1.Int64N(100)))
		if err != nil {
			log.Fatal(i, err)
		}
	}

	val = testTree.findKV(7811) //returns 23
	if val == 0 || val != 23 {
		log.Fatal(errorBadFind, 7811, val)
	}

	val = testTree.findKV(7922) //returns 2
	if val == 0 || val != 2 {
		log.Fatal(errorBadFind, 7922, val)
	}

	val = testTree.findKV(7999) //returns 44
	if val == 0 || val != 44 {
		log.Fatal(errorBadFind, 7999, val)
	}

	val = testTree.findKV(7451) //returns 31
	if val == 0 || val != 31 {
		log.Fatal(errorBadFind, 7451, val)
	}

	val = testTree.findKV(7555) //returns 8
	if val == 0 || val != 8 {
		log.Fatal(errorBadFind, 7555, val)
	}

	val = testTree.findKV(9000) //returns 0
	if val != 0 {
		log.Fatal(errorBadFind, 9000, val)
	}

	val = testTree.findKV(1000) //returns 75
	if val == 0 || val != 75 {
		log.Fatal(errorBadFind, 1000, val)
	}

	firstNode := testTree.firstLeafNode()
	if firstNode == nil {
		log.Fatal("cannot find first leaf node")
	}
	secondNode := firstNode.nextLeaf()
	if secondNode == nil {
		log.Fatal("leaf nodes not linked to second one")
	}
	nodeKeys := secondNode.keys()
	nodeValues := secondNode.values()
	nodeBytes := secondNode.toBytes()

	if len(nodeBytes) != 4096 {
		log.Fatal("leaf node size is not 4096")
	}

	err = firstNode.fromBytes(nodeBytes)
	if err != nil {
		log.Fatal(err)
	}
	tempNodeKeys := firstNode.keys()
	tempNodeValues := firstNode.values()

	require.Equal(t, nodeKeys, tempNodeKeys)
	require.Equal(t, nodeValues, tempNodeValues)

	middleNode := testTree.findNode(1000)
	if middleNode == nil {
		log.Fatal("leaf node not found for middle")
	}
	tempBytes := middleNode.toBytes()
	if len(tempBytes) != 4096 {
		log.Fatal("leaf node size is not 4096")
	}
	nodeKeys = middleNode.keys()
	found := false
	for i := range nodeKeys {
		if nodeKeys[i] == 1000 {
			found = true
			break
		}
	}
	if !found {
		log.Fatal("key not in keys list of leaf node")
	}
}

func TestBPTree3232(t *testing.T) {
	// Initialize tree here

	//EX: newtree := InitializeTree() where InitializeTree returns your intialized tree
	var testTree BPTree3232 = nil //replace nil with your newtree should compile if your tree struct properly defines the interface

	// Do not modify code below except for adding debug logs
	if testTree == nil {
		log.Fatal("bptree not initialized")
	}
	r1 := rand.New(rand.NewPCG(42, 1024))
	for i := uint32(1); i < 200; i++ {
		err := testTree.insertNodeKV(i, uint32(r1.Int64N(100)))
		if err != nil {
			log.Fatal(i, err)
		}
	}
	err := testTree.insertNodeKV(202, 256)
	if err != nil {
		log.Fatal(err)
	}

	val := testTree.findKV(202)
	if val == 0 || val != 256 {
		log.Fatal(errorBadFind, 202, val)
	}

	val = testTree.findKV(201)
	if val != 0 {
		log.Fatal(errorBadFind, 201, val)
	}

	err = testTree.insertNodeKV(17, 256)
	if err == nil {
		log.Fatal("number passed through nonunique")
	}

	for i := uint32(203); i < 8000; i++ {
		err := testTree.insertNodeKV(i, uint32(r1.Int64N(100)))
		if err != nil {
			log.Fatal(i, err)
		}
	}

	val = testTree.findKV(7811) //returns 23
	if val == 0 || val != 23 {
		log.Fatal(errorBadFind, 7811, val)
	}

	val = testTree.findKV(7922) //returns 2
	if val == 0 || val != 2 {
		log.Fatal(errorBadFind, 7922, val)
	}

	val = testTree.findKV(7999) //returns 44
	if val == 0 || val != 44 {
		log.Fatal(errorBadFind, 7999, val)
	}

	val = testTree.findKV(7451) //returns 31
	if val == 0 || val != 31 {
		log.Fatal(errorBadFind, 7451, val)
	}

	val = testTree.findKV(7555) //returns 8
	if val == 0 || val != 8 {
		log.Fatal(errorBadFind, 7555, val)
	}

	val = testTree.findKV(9000) //returns 0
	if val != 0 {
		log.Fatal(errorBadFind, 9000, val)
	}

	val = testTree.findKV(1000) //returns 75
	if val == 0 || val != 75 {
		log.Fatal(errorBadFind, 1000, val)
	}

	firstNode := testTree.firstLeafNode()
	if firstNode == nil {
		log.Fatal("cannot find first leaf node")
	}
	secondNode := firstNode.nextLeaf()
	if secondNode == nil {
		log.Fatal("leaf nodes not linked to second one")
	}
	nodeKeys := secondNode.keys()
	nodeValues := secondNode.values()
	nodeBytes := secondNode.toBytes()

	if len(nodeBytes) != 4096 {
		log.Fatal("leaf node size is not 4096")
	}

	err = firstNode.fromBytes(nodeBytes)
	if err != nil {
		log.Fatal(err)
	}
	tempNodeKeys := firstNode.keys()
	tempNodeValues := firstNode.values()

	require.Equal(t, nodeKeys, tempNodeKeys)
	require.Equal(t, nodeValues, tempNodeValues)

	middleNode := testTree.findNode(1000)
	if middleNode == nil {
		log.Fatal("leaf node not found for middle")
	}
	tempBytes := middleNode.toBytes()
	if len(tempBytes) != 4096 {
		log.Fatal("leaf node size is not 4096")
	}
	nodeKeys = middleNode.keys()
	found := false
	for i := range nodeKeys {
		if nodeKeys[i] == 1000 {
			found = true
			break
		}
	}
	if !found {
		log.Fatal("key not in keys list of leaf node")
	}
}

func TestBPTreeUnique(t *testing.T) {
	// Initialize tree here

	//EX: newtree := InitializeTree() where InitializeTree returns your intialized tree
	var testTree BPTreeUnique = nil //replace nil with your newtree should compile if your tree struct properly defines the interface

	// Do not modify code below except for adding debug logs
	if testTree == nil {
		log.Fatal("bptree not initialized")
	}
	for i := int64(1); i < 200; i++ {
		err := testTree.insertKey(i)
		if err != nil {
			log.Fatal(i, err)
		}
	}
	err := testTree.insertKey(202)
	if err != nil {
		log.Fatal(err)
	}

	val := testTree.keyExists(202)
	if !val {
		log.Fatal(errorBadFind, true, val)
	}

	val = testTree.keyExists(201)
	if val {
		log.Fatal(errorBadFind, false, val)
	}

	err = testTree.insertKey(17)
	if err == nil {
		log.Fatal("number passed through nonunique")
	}

	for i := int64(203); i < 8000; i++ {
		err := testTree.insertKey(i)
		if err != nil {
			log.Fatal(i, err)
		}
	}

	val = testTree.keyExists(7811)
	if !val {
		log.Fatal(errorBadFind, true, val)
	}

	val = testTree.keyExists(7922)
	if !val {
		log.Fatal(errorBadFind, true, val)
	}

	val = testTree.keyExists(7999)
	if !val {
		log.Fatal(errorBadFind, true, val)
	}

	val = testTree.keyExists(7451)
	if !val {
		log.Fatal(errorBadFind, true, val)
	}

	val = testTree.keyExists(7555)
	if !val {
		log.Fatal(errorBadFind, true, val)
	}

	val = testTree.keyExists(9000)
	if val {
		log.Fatal(errorBadFind, false, val)
	}

	val = testTree.keyExists(1000)
	if !val {
		log.Fatal(errorBadFind, true, val)
	}

	firstNode := testTree.firstLeafNode()
	if firstNode == nil {
		log.Fatal("cannot find first leaf node")
	}
	secondNode := firstNode.nextLeaf()
	if secondNode == nil {
		log.Fatal("leaf nodes not linked to second one")
	}
	nodeKeys := secondNode.keys()
	nodeBytes := secondNode.toBytes()

	if len(nodeBytes) != 4096 {
		log.Fatal("leaf node size is not 4096")
	}

	err = firstNode.fromBytes(nodeBytes)
	if err != nil {
		log.Fatal(err)
	}
	tempNodeKeys := firstNode.keys()

	require.Equal(t, nodeKeys, tempNodeKeys)

	middleNode := testTree.findNode(1000)
	if middleNode == nil {
		log.Fatal("leaf node not found for middle")
	}
	tempBytes := middleNode.toBytes()
	if len(tempBytes) != 4096 {
		log.Fatal("leaf node size is not 4096")
	}
	nodeKeys = middleNode.keys()
	found := false
	for i := range nodeKeys {
		if nodeKeys[i] == 1000 {
			found = true
			break
		}
	}
	if !found {
		log.Fatal("key not in keys list of leaf node")
	}
}
