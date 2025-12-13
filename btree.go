package nnut

import (
	"fmt"
	"slices"
	"sort"
	"sync"

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
	n.Keys = slices.Insert(n.Keys, bCount, aChildren.Keys[mid])
	n.Values = slices.Insert(n.Values, bCount, aChildren.Values[mid])

	// Insert z as new child
	n.Children = slices.Insert(n.Children, bCount+1, bChildren)

	// Truncate y
	aChildren.Keys = aChildren.Keys[:mid]
	aChildren.Values = aChildren.Values[:mid]
}

// insertNonFull inserts a key-value pair into a non-full node
func (n *BTreeNode) insertNonFull(t int, key string, value string) {
	if len(n.Keys) != len(n.Values) {
		panic(fmt.Sprintf("BTree invariant violated: len(Keys)=%d, len(Values)=%d", len(n.Keys), len(n.Values)))
	}
	if !n.IsLeaf && len(n.Children) != len(n.Keys)+1 {
		panic(fmt.Sprintf("BTree invariant violated: leaf=%v, len(Keys)=%d, len(Children)=%d", n.IsLeaf, len(n.Keys), len(n.Children)))
	}
	i := sort.SearchStrings(n.Keys, key)

	if n.IsLeaf {
		if i < len(n.Keys) && n.Keys[i] == key {
			// Key exists, append to existing list
			n.Values[i] = append(n.Values[i], value)
		} else {
			// Insert new key
			n.Keys = slices.Insert(n.Keys, i, key)
			n.Values = slices.Insert(n.Values, i, []string{value})
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
	mu              sync.RWMutex
	dirty           bool
	version         uint64
}

// BTreeItem represents a key-value pair for bulk operations
type BTreeItem struct {
	Key   string
	Value string
}

// BTreeIterator provides efficient iteration over B-tree range queries
type BTreeIterator struct {
	tree          *BTree
	min, max      string
	includeMin    bool
	includeMax    bool
	path          []iteratorNode
	currentValues []string
	valueIndex    int
	finished      bool
}

type iteratorNode struct {
	node  *BTreeNode
	index int
}

// persistedBTree represents the serialized format of a B-tree
type persistedBTree struct {
	Version   uint64     `msgpack:"version"`
	Branching int        `msgpack:"branching"`
	Root      *BTreeNode `msgpack:"root"`
}

// NewBTreeIterator creates a new iterator for range queries
func NewBTreeIterator(tree *BTree, min, max string, includeMin, includeMax bool) *BTreeIterator {
	it := &BTreeIterator{
		tree:       tree,
		min:        min,
		max:        max,
		includeMin: includeMin,
		includeMax: includeMax,
		path:       make([]iteratorNode, 0),
		finished:   false,
	}
	it.findStart()
	it.advance()
	return it
}

// findStart builds the path to the starting position for the range
func (it *BTreeIterator) findStart() {
	node := it.tree.Root
	if node == nil {
		it.finished = true
		return
	}
	for !node.IsLeaf {
		i := 0
		if it.min != "" {
			i = sort.SearchStrings(node.Keys, it.min)
		}
		it.path = append(it.path, iteratorNode{node: node, index: i})
		node = node.Children[i]
	}
	// At leaf
	startIndex := 0
	if it.min != "" {
		startIndex = sort.SearchStrings(node.Keys, it.min)
	}
	it.path = append(it.path, iteratorNode{node: node, index: startIndex})
}

// advance moves to the next valid key-value pair in the range
func (it *BTreeIterator) advance() {
	if it.finished {
		return
	}

	for len(it.path) > 0 {
		current := &it.path[len(it.path)-1]
		node := current.node

		if node.IsLeaf {
			// Process keys in this leaf
			for current.index < len(node.Keys) {
				key := node.Keys[current.index]
				if it.isKeyGreaterThanMax(key) {
					it.finished = true
					return
				}
				if it.isInRange(key) {
					it.currentValues = node.Values[current.index]
					it.valueIndex = 0
					current.index++
					return
				}
				current.index++
			}
			// Leaf exhausted, pop it
			it.path = it.path[:len(it.path)-1]
		} else {
			// Internal node, move to next child
			current.index++
			if current.index < len(node.Children) {
				// Check for subtree pruning
				if it.isSubtreeLessThanMin(node, current.index) {
					// Skip this subtree entirely
					continue
				}
				if it.isSubtreeGreaterThanMax(node, current.index) {
					// Entire remaining subtrees are > max, terminate
					it.finished = true
					return
				}
				// Descend to leftmost leaf of this child
				child := node.Children[current.index]
				for !child.IsLeaf {
					it.path = append(it.path, iteratorNode{node: child, index: 0})
					child = child.Children[0]
				}
				it.path = append(it.path, iteratorNode{node: child, index: 0})
			} else {
				// No more children, pop this node
				it.path = it.path[:len(it.path)-1]
			}
		}
	}

	it.finished = true
}

// isInRange checks if a key is within the iterator's range bounds
func (it *BTreeIterator) isInRange(key string) bool {
	if it.min != "" {
		if it.includeMin {
			if key < it.min {
				return false
			}
		} else {
			if key <= it.min {
				return false
			}
		}
	}
	if it.max != "" {
		if it.includeMax {
			if key > it.max {
				return false
			}
		} else {
			if key >= it.max {
				return false
			}
		}
	}
	return true
}

// isKeyGreaterThanMax checks if a key exceeds the max bound for early termination
func (it *BTreeIterator) isKeyGreaterThanMax(key string) bool {
	if it.max == "" {
		return false
	}
	if it.includeMax {
		return key > it.max
	}
	return key >= it.max
}

// isSubtreeLessThanMin checks if an entire subtree is less than the min bound
func (it *BTreeIterator) isSubtreeLessThanMin(node *BTreeNode, childIndex int) bool {
	if it.min == "" {
		return false
	}
	// For child i, max key in subtree is node.Keys[i] if i < len(Keys)
	if childIndex >= len(node.Keys) {
		return false // Last child, no upper bound
	}
	maxInSubtree := node.Keys[childIndex]
	if it.includeMin {
		return maxInSubtree < it.min
	}
	return maxInSubtree <= it.min
}

// isSubtreeGreaterThanMax checks if an entire subtree is greater than the max bound
func (it *BTreeIterator) isSubtreeGreaterThanMax(node *BTreeNode, childIndex int) bool {
	if it.max == "" {
		return false
	}
	// For child i, min key in subtree is node.Keys[i-1] if i > 0
	if childIndex == 0 {
		return false // First child, no lower bound
	}
	minInSubtree := node.Keys[childIndex-1]
	return it.isKeyGreaterThanMax(minInSubtree)
}

// HasNext returns true if there are more values to iterate
func (it *BTreeIterator) HasNext() bool {
	return !it.finished && it.valueIndex < len(it.currentValues)
}

// Next returns the next value in the iteration
func (it *BTreeIterator) Next() string {
	if !it.HasNext() {
		return ""
	}
	value := it.currentValues[it.valueIndex]
	it.valueIndex++
	if it.valueIndex >= len(it.currentValues) {
		it.advance()
	}
	return value
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

func (t *BTree) minKeys() int {
	return t.BranchingFactor - 1
}

func (n *BTreeNode) isUnderfilled(t int, isRoot bool) bool {
	if isRoot && len(n.Children) == 0 {
		return false // root can be empty
	}
	return len(n.Keys) < t-1
}

func (n *BTreeNode) removeKey(i int) {
	n.Keys = slices.Delete(n.Keys, i, i+1)
	n.Values = slices.Delete(n.Values, i, i+1)
	if !n.IsLeaf {
		n.Children = slices.Delete(n.Children, i+1, i+2)
	}
}

func (t *BTree) borrowFromLeft(parent *BTreeNode, childIndex int) {
	node := parent.Children[childIndex]
	leftSibling := parent.Children[childIndex-1]

	// Move parent's separator key down to node
	node.Keys = slices.Insert(node.Keys, 0, parent.Keys[childIndex-1])
	node.Values = slices.Insert(node.Values, 0, parent.Values[childIndex-1])

	// Move left sibling's last key up to parent
	last := len(leftSibling.Keys) - 1
	parent.Keys[childIndex-1] = leftSibling.Keys[last]
	parent.Values[childIndex-1] = leftSibling.Values[last]

	// Remove from left sibling
	leftSibling.Keys = slices.Delete(leftSibling.Keys, last, last+1)
	leftSibling.Values = slices.Delete(leftSibling.Values, last, last+1)

	// Move children if not leaf
	if !node.IsLeaf {
		node.Children = slices.Insert(node.Children, 0, leftSibling.Children[len(leftSibling.Children)-1])
		leftSibling.Children = slices.Delete(leftSibling.Children, len(leftSibling.Children)-1, len(leftSibling.Children))
	}
}

func (t *BTree) borrowFromRight(parent *BTreeNode, childIndex int) {
	node := parent.Children[childIndex]
	rightSibling := parent.Children[childIndex+1]

	// Move parent's separator key down to node
	node.Keys = slices.Insert(node.Keys, len(node.Keys), parent.Keys[childIndex])
	node.Values = slices.Insert(node.Values, len(node.Values), parent.Values[childIndex])

	// Move right sibling's first key up to parent
	parent.Keys[childIndex] = rightSibling.Keys[0]
	parent.Values[childIndex] = rightSibling.Values[0]

	// Remove from right sibling
	rightSibling.Keys = slices.Delete(rightSibling.Keys, 0, 1)
	rightSibling.Values = slices.Delete(rightSibling.Values, 0, 1)

	// Move children if not leaf
	if !node.IsLeaf {
		node.Children = slices.Insert(node.Children, len(node.Children), rightSibling.Children[0])
		rightSibling.Children = slices.Delete(rightSibling.Children, 0, 1)
	}
}

func (t *BTree) mergeWithLeft(parent *BTreeNode, childIndex int) {
	node := parent.Children[childIndex]
	leftSibling := parent.Children[childIndex-1]

	// Move parent's separator key down to left sibling
	leftSibling.Keys = slices.Insert(leftSibling.Keys, len(leftSibling.Keys), parent.Keys[childIndex-1])
	leftSibling.Values = slices.Insert(leftSibling.Values, len(leftSibling.Values), parent.Values[childIndex-1])

	// Move all keys and values from node to left sibling
	leftSibling.Keys = append(leftSibling.Keys, node.Keys...)
	leftSibling.Values = append(leftSibling.Values, node.Values...)

	// Move children if not leaf
	if !node.IsLeaf {
		leftSibling.Children = append(leftSibling.Children, node.Children...)
	}

	// Remove separator from parent
	parent.Keys = slices.Delete(parent.Keys, childIndex-1, childIndex)
	parent.Values = slices.Delete(parent.Values, childIndex-1, childIndex)

	// Remove node from parent's children
	parent.Children = slices.Delete(parent.Children, childIndex, childIndex+1)
}

func (t *BTree) mergeWithRight(parent *BTreeNode, childIndex int) {
	node := parent.Children[childIndex]
	rightSibling := parent.Children[childIndex+1]

	// Move parent's separator key down to node
	node.Keys = slices.Insert(node.Keys, len(node.Keys), parent.Keys[childIndex])
	node.Values = slices.Insert(node.Values, len(node.Values), parent.Values[childIndex])

	// Move all keys and values from right sibling to node
	node.Keys = append(node.Keys, rightSibling.Keys...)
	node.Values = append(node.Values, rightSibling.Values...)

	// Move children if not leaf
	if !node.IsLeaf {
		node.Children = append(node.Children, rightSibling.Children...)
	}

	// Remove separator from parent
	parent.Keys = slices.Delete(parent.Keys, childIndex, childIndex+1)
	parent.Values = slices.Delete(parent.Values, childIndex, childIndex+1)

	// Remove right sibling from parent's children
	parent.Children = slices.Delete(parent.Children, childIndex+1, childIndex+2)
}

func (t *BTree) rebalance(parent *BTreeNode, childIndex int) {
	if parent == nil {
		return // root
	}

	node := parent.Children[childIndex]
	if !node.isUnderfilled(t.BranchingFactor, false) {
		return
	}

	// Try to borrow from left sibling
	if childIndex > 0 {
		leftSibling := parent.Children[childIndex-1]
		if len(leftSibling.Keys) > t.minKeys() {
			t.borrowFromLeft(parent, childIndex)
			return
		}
	}

	// Try to borrow from right sibling
	if childIndex < len(parent.Children)-1 {
		rightSibling := parent.Children[childIndex+1]
		if len(rightSibling.Keys) > t.minKeys() {
			t.borrowFromRight(parent, childIndex)
			return
		}
	}

	// Merge with sibling
	if childIndex > 0 {
		t.mergeWithLeft(parent, childIndex)
	} else if childIndex < len(parent.Children)-1 {
		t.mergeWithRight(parent, childIndex)
	} else {
		// This should not happen in a valid B-tree
		// The node is underfilled but has no siblings to borrow from or merge with
		// This indicates a bug in the implementation
		panic("B-tree invariant violated: underfilled node with no siblings")
	}
}

func (t *BTree) findPredecessor(node *BTreeNode) (string, []string) {
	if node.IsLeaf {
		last := len(node.Keys) - 1
		return node.Keys[last], node.Values[last]
	}
	return t.findPredecessor(node.Children[len(node.Children)-1])
}

func (t *BTree) removeKeyFromSubtree(parent *BTreeNode, node *BTreeNode, index int, key string) {
	i := sort.SearchStrings(node.Keys, key)
	if i < len(node.Keys) && node.Keys[i] == key {
		if node.IsLeaf {
			node.removeKey(i)
			t.rebalance(parent, index)
		} else {
			// Replace with predecessor
			predKey, predValues := t.findPredecessor(node.Children[i])
			node.Keys[i] = predKey
			node.Values[i] = predValues
			t.removeKeyFromSubtree(node, node.Children[i], i, predKey)
			t.rebalance(node, i)
		}
		return
	}
	if !node.IsLeaf {
		t.removeKeyFromSubtree(node, node.Children[i], i, key)
	}
}

func (t *BTree) delete(parent *BTreeNode, node *BTreeNode, index int, key string, value string) {
	i := sort.SearchStrings(node.Keys, key)
	if i < len(node.Keys) && node.Keys[i] == key {
		if node.IsLeaf {
			// Remove the specific value
			values := node.Values[i]
			for j, v := range values {
				if v == value {
					node.Values[i] = append(values[:j], values[j+1:]...)
					break
				}
			}
			if len(node.Values[i]) == 0 {
				node.removeKey(i)
			}
			t.rebalance(parent, index)
		} else {
			// Internal node: replace with predecessor
			predKey, predValues := t.findPredecessor(node.Children[i])
			node.Keys[i] = predKey
			node.Values[i] = predValues
			t.removeKeyFromSubtree(node, node.Children[i], i, predKey)
			t.rebalance(parent, index)
		}
		return
	}
	if !node.IsLeaf {
		t.delete(node, node.Children[i], i, key, value)
	}
	t.rebalance(parent, index)
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
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.countKeys(t.Root)
}

// CountUniqueValues returns the number of unique index values in the B-tree
func (t *BTree) CountUniqueValues() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.countUnique(t.Root)
}

// Delete removes a record key from the index under the given index value
func (t *BTree) Delete(indexValue string, recordKey string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.delete(nil, t.Root, 0, indexValue, recordKey)
	// If root has no keys and has children, make the child the new root
	if len(t.Root.Keys) == 0 && len(t.Root.Children) == 1 {
		t.Root = t.Root.Children[0]
	}
	t.dirty = true
	t.version++
}

// GetAllKeys returns all record keys in order of index values
func (t *BTree) GetAllKeys() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var result []string
	t.getAllKeys(t.Root, &result)
	return result
}

// Insert adds a record key to the index under the given index value
func (t *BTree) Insert(indexValue string, recordKey string) {
	t.mu.Lock()
	defer t.mu.Unlock()
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
	t.dirty = true
	t.version++
}

// RangeSearch finds all record keys for index values in the given range
func (t *BTree) RangeSearch(min string, max string, includeMin bool, includeMax bool) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	it := NewBTreeIterator(t, min, max, includeMin, includeMax)
	var result []string
	for it.HasNext() {
		result = append(result, it.Next())
	}
	return result
}

// rangeSearch traverses the tree to find all keys in range (legacy implementation)
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

// RangeCount returns the number of record keys in the given range
func (t *BTree) RangeCount(min string, max string, includeMin bool, includeMax bool) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var result []string
	t.rangeSearch(t.Root, min, max, includeMin, includeMax, &result)
	return len(result)
}

// BulkInsert inserts multiple key-value pairs efficiently
func (t *BTree) BulkInsert(items []BTreeItem) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Sort items by key for optimal insertion order
	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})

	for _, item := range items {
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
		t.Root.insertNonFull(t.BranchingFactor, item.Key, item.Value)
	}
}

// BulkDelete deletes multiple key-value pairs efficiently
func (t *BTree) BulkDelete(items []BTreeItem) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Sort items by key for optimal deletion order
	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})

	for _, item := range items {
		t.delete(nil, t.Root, 0, item.Key, item.Value)
		// Handle root becoming empty
		if len(t.Root.Keys) == 0 && len(t.Root.Children) == 1 {
			t.Root = t.Root.Children[0]
		}
	}
}

// BulkSearch performs multiple equality searches efficiently
func (t *BTree) BulkSearch(keys []string) map[string][]string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make(map[string][]string, len(keys))
	for _, key := range keys {
		result[key] = t.search(t.Root, key)
	}
	return result
}

// Search finds all record keys for a given index value
func (t *BTree) Search(indexValue string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.search(t.Root, indexValue)
}

// Serialize encodes the B-tree to msgpack bytes with versioning
func (t *BTree) Serialize() ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	pb := persistedBTree{
		Version:   t.version,
		Branching: t.BranchingFactor,
		Root:      t.Root,
	}

	data, err := msgpack.Marshal(pb)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Deserialize decodes the B-tree from msgpack bytes
func deserializeBTreeIndex(data []byte) (*BTree, error) {
	var pb persistedBTree
	err := msgpack.Unmarshal(data, &pb)
	if err != nil {
		return nil, err
	}

	t := &BTree{
		Root:            pb.Root,
		BranchingFactor: pb.Branching,
		version:         pb.Version,
		dirty:           false, // Loaded from disk, not dirty
	}

	return t, nil
}
