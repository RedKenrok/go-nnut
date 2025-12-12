package nnut

import (
	"sort"

	"github.com/vmihailenco/msgpack/v5"
)

// BTreeNode represents a node in the B-tree index
type BTreeNode struct {
	Keys     []string     // sorted indexed values (strings)
	Values   [][]string   // for each key, list of record keys
	Children []*BTreeNode // child nodes (len = len(Keys)+1 for internal nodes)
	IsLeaf   bool
}

// isFull returns true if the node has reached maximum capacity
func (n *BTreeNode) isFull(t int) bool {
	return len(n.Keys) >= 2*t-1
}

// splitChild splits a full child node
func (n *BTreeNode) splitChild(aCount int, bCount int) {
	aChildren := n.Children[bCount]
	bChildren := &BTreeNode{
		Keys:     make([]string, 0, 2*aCount-1),
		Values:   make([][]string, 0, 2*aCount-1),
		Children: make([]*BTreeNode, 0, 2*aCount),
		IsLeaf:   aChildren.IsLeaf,
	}

	// Move t keys and values from y to z
	mid := aCount - 1
	bChildren.Keys = append(bChildren.Keys, aChildren.Keys[mid+1:]...)
	bChildren.Values = append(bChildren.Values, aChildren.Values[mid+1:]...)

	// Move children if not leaf
	if !aChildren.IsLeaf {
		bChildren.Children = append(bChildren.Children, aChildren.Children[mid+1:]...)
		aChildren.Children = aChildren.Children[:mid+1]
	}

	// Move middle key to parent
	n.Keys = append(n.Keys[:bCount], append([]string{aChildren.Keys[mid]}, n.Keys[bCount:]...)...)
	n.Values = append(n.Values[:bCount], append([][]string{aChildren.Values[mid]}, n.Values[bCount:]...)...)

	// Insert z as new child
	n.Children = append(n.Children[:bCount+1], append([]*BTreeNode{bChildren}, n.Children[bCount+1:]...)...)

	// Truncate y
	aChildren.Keys = aChildren.Keys[:mid]
	aChildren.Values = aChildren.Values[:mid]
}

// insertNonFull inserts a key-value pair into a non-full node
func (n *BTreeNode) insertNonFull(t int, key string, value string) {
	i := sort.SearchStrings(n.Keys, key)

	if n.IsLeaf {
		if i < len(n.Keys) && n.Keys[i] == key {
			// Key exists, append to existing list
			n.Values[i] = append(n.Values[i], value)
		} else {
			// Insert new key
			n.Keys = append(n.Keys[:i], append([]string{key}, n.Keys[i:]...)...)
			n.Values = append(n.Values[:i], append([][]string{{value}}, n.Values[i:]...)...)
		}
	} else {
		// Descend to child
		child := n.Children[i]
		if child.isFull(t) {
			n.splitChild(t, i)
			if key > n.Keys[i] {
				i++
			}
		}
		n.Children[i].insertNonFull(t, key, value)
	}
}

// BTree implements a B-tree for efficient indexing
type BTree struct {
	Root            *BTreeNode
	BranchingFactor int // t, where max keys per node = 2t-1, min = t-1
}

// NewBTreeIndex creates a new B-tree index with the given branching factor
func NewBTreeIndex(branchingFactor int) *BTree {
	if branchingFactor < 2 {
		branchingFactor = 32 // default
	}
	return &BTree{
		Root: &BTreeNode{
			Keys:     make([]string, 0),
			Values:   make([][]string, 0),
			Children: make([]*BTreeNode, 0),
			IsLeaf:   true,
		},
		BranchingFactor: branchingFactor,
	}
}

func (t *BTree) countKeys(node *BTreeNode) int {
	if node == nil {
		return 0
	}
	count := 0
	for _, values := range node.Values {
		count += len(values)
	}
	if !node.IsLeaf {
		for _, child := range node.Children {
			count += t.countKeys(child)
		}
	}
	return count
}

func (t *BTree) countUnique(node *BTreeNode) int {
	if node == nil {
		return 0
	}
	count := len(node.Keys)
	if !node.IsLeaf {
		for _, child := range node.Children {
			count += t.countUnique(child)
		}
	}
	return count
}

func (t *BTree) delete(node *BTreeNode, key string, value string) {
	i := sort.SearchStrings(node.Keys, key)
	if i < len(node.Keys) && node.Keys[i] == key {
		// Found the key, remove the value from the list
		values := node.Values[i]
		for j, v := range values {
			if v == value {
				node.Values[i] = append(values[:j], values[j+1:]...)
				break
			}
		}
		// If list is empty, remove the key (simplified, no rebalancing yet)
		if len(node.Values[i]) == 0 {
			node.Keys = append(node.Keys[:i], node.Keys[i+1:]...)
			node.Values = append(node.Values[:i], node.Values[i+1:]...)
			if !node.IsLeaf {
				node.Children = append(node.Children[:i+1], node.Children[i+2:]...)
			}
		}
		return
	}
	if !node.IsLeaf {
		t.delete(node.Children[i], key, value)
	}
}

func (t *BTree) getAllKeys(node *BTreeNode, result *[]string) {
	if node == nil {
		return
	}
	i := 0
	if !node.IsLeaf {
		t.getAllKeys(node.Children[i], result)
	}
	for ; i < len(node.Keys); i++ {
		*result = append(*result, node.Values[i]...)
		if !node.IsLeaf {
			t.getAllKeys(node.Children[i+1], result)
		}
	}
}

func (t *BTree) rangeSearch(node *BTreeNode, min string, max string, includeMin bool, includeMax bool, result *[]string) {
	if node == nil {
		return
	}

	for i, key := range node.Keys {
		if (min == "" || (includeMin && key >= min) || (!includeMin && key > min)) &&
			(max == "" || (includeMax && key <= max) || (!includeMax && key < max)) {
			*result = append(*result, node.Values[i]...)
		}
		if !node.IsLeaf {
			t.rangeSearch(node.Children[i], min, max, includeMin, includeMax, result)
		}
	}
	if !node.IsLeaf {
		t.rangeSearch(node.Children[len(node.Children)-1], min, max, includeMin, includeMax, result)
	}
}

func (t *BTree) search(node *BTreeNode, key string) []string {
	i := sort.SearchStrings(node.Keys, key)
	if i < len(node.Keys) && node.Keys[i] == key {
		return node.Values[i]
	}
	if node.IsLeaf {
		return nil
	}
	return t.search(node.Children[i], key)
}

// CountKeys returns the total number of record keys in the B-tree
func (t *BTree) CountKeys() int {
	return t.countKeys(t.Root)
}

// CountUniqueValues returns the number of unique index values in the B-tree
func (t *BTree) CountUniqueValues() int {
	return t.countUnique(t.Root)
}

// Delete removes a record key from the index under the given index value
func (t *BTree) Delete(indexValue string, recordKey string) {
	t.delete(t.Root, indexValue, recordKey)
}

// GetAllKeys returns all record keys in order of index values
func (t *BTree) GetAllKeys() []string {
	var result []string
	t.getAllKeys(t.Root, &result)
	return result
}

// Insert adds a record key to the index under the given index value
func (t *BTree) Insert(indexValue string, recordKey string) {
	root := t.Root
	if root.isFull(t.BranchingFactor) {
		// Split root
		newRoot := &BTreeNode{
			Keys:     make([]string, 0),
			Values:   make([][]string, 0),
			Children: []*BTreeNode{root},
			IsLeaf:   false,
		}
		newRoot.splitChild(t.BranchingFactor, 0)
		t.Root = newRoot
	}
	t.Root.insertNonFull(t.BranchingFactor, indexValue, recordKey)
}

// RangeSearch finds all record keys for index values in the given range
func (t *BTree) RangeSearch(min string, max string, includeMin bool, includeMax bool) []string {
	var result []string
	t.rangeSearch(t.Root, min, max, includeMin, includeMax, &result)
	return result
}

// Search finds all record keys for a given index value
func (t *BTree) Search(indexValue string) []string {
	return t.search(t.Root, indexValue)
}

// Serialize encodes the B-tree to msgpack bytes
func (t *BTree) Serialize() ([]byte, error) {
	return msgpack.Marshal(t)
}

// Deserialize decodes the B-tree from msgpack bytes
func deserializeBTreeIndex(data []byte) (*BTree, error) {
	var t BTree
	err := msgpack.Unmarshal(data, &t)
	if err != nil {
		return nil, err
	}
	return &t, nil
}
