package nnut

import (
	"slices"
	"sort"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

// bTree implements a B-tree for efficient indexing
type bTree struct {
	Root            *bTreeNode
	BranchingFactor int // t, where max keys per node = 2t-1, min = t-1
	mutex           sync.RWMutex
	dirty           bool
	version         uint64
}

// bTreeItem represents a key-value pair for bulk operations
type bTreeItem struct {
	Key   string
	Value string
}

// persistedBTree represents the serialized format of a B-tree
type persistedBTree struct {
	Version   uint64     `msgpack:"version"`
	Branching int        `msgpack:"branching"`
	Root      *bTreeNode `msgpack:"root"`
}

// newBTree creates a new B-tree index with the given branching factor
func newBTree(branchingFactor int) *bTree {
	if branchingFactor < 2 {
		branchingFactor = 32 // default
	}
	return &bTree{
		Root: &bTreeNode{
			Keys:     make([]string, 0),
			Values:   make([][]string, 0),
			Children: make([]*bTreeNode, 0),
			IsLeaf:   true,
		},
		BranchingFactor: branchingFactor,
	}
}

func (t *bTree) countKeysRecursive(node *bTreeNode) int {
	if node == nil {
		return 0
	}
	count := 0
	for _, values := range node.Values {
		count += len(values)
	}
	if !node.IsLeaf {
		for _, child := range node.Children {
			count += t.countKeysRecursive(child)
		}
	}
	return count
}

func (t *bTree) countUniqueRecursive(node *bTreeNode) int {
	if node == nil {
		return 0
	}
	count := len(node.Keys)
	if !node.IsLeaf {
		for _, child := range node.Children {
			count += t.countUniqueRecursive(child)
		}
	}
	return count
}

func (t *bTree) minKeys() int {
	return t.BranchingFactor - 1
}

func (t *bTree) borrowFromLeft(parent *bTreeNode, childIndex int) {
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

func (t *bTree) borrowFromRight(parent *bTreeNode, childIndex int) {
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

func (t *bTree) mergeWithLeft(parent *bTreeNode, childIndex int) {
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

func (t *bTree) mergeWithRight(parent *bTreeNode, childIndex int) {
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

func (t *bTree) rebalance(parent *bTreeNode, childIndex int) {
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

func (t *bTree) findPredecessor(node *bTreeNode) (string, []string) {
	if node.IsLeaf {
		last := len(node.Keys) - 1
		return node.Keys[last], node.Values[last]
	}
	return t.findPredecessor(node.Children[len(node.Children)-1])
}

func (t *bTree) removeKeyFromSubtree(parent *bTreeNode, node *bTreeNode, index int, key string) {
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

func (t *bTree) deleteRecursive(parent *bTreeNode, node *bTreeNode, index int, key string, value string) {
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
		t.deleteRecursive(node, node.Children[i], i, key, value)
	}
	t.rebalance(parent, index)
}

func (t *bTree) getAllKeysRecursive(node *bTreeNode, result *[]string) {
	if node == nil {
		return
	}
	i := 0
	if !node.IsLeaf {
		t.getAllKeysRecursive(node.Children[i], result)
	}
	for ; i < len(node.Keys); i++ {
		*result = append(*result, node.Values[i]...)
		if !node.IsLeaf {
			t.getAllKeysRecursive(node.Children[i+1], result)
		}
	}
}

func (t *bTree) searchRecursive(node *bTreeNode, key string) []string {
	i := sort.SearchStrings(node.Keys, key)
	if i < len(node.Keys) && node.Keys[i] == key {
		return node.Values[i]
	}
	if node.IsLeaf {
		return nil
	}
	return t.searchRecursive(node.Children[i], key)
}

// countKeys returns the total number of record keys in the B-tree
func (t *bTree) countKeys() int {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.countKeysRecursive(t.Root)
}

// countUniqueValues returns the number of unique index values in the B-tree
func (t *bTree) countUniqueValues() int {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.countUniqueRecursive(t.Root)
}

// delete removes a record key from the index under the given index value
func (t *bTree) delete(indexValue string, recordKey string) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.deleteRecursive(nil, t.Root, 0, indexValue, recordKey)
	// If root has no keys and has children, make the child the new root
	if len(t.Root.Keys) == 0 && len(t.Root.Children) == 1 {
		t.Root = t.Root.Children[0]
	}
	t.dirty = true
	t.version++
}

// getAllKeys returns all record keys in order of index values
func (t *bTree) getAllKeys() []string {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	var result []string
	t.getAllKeysRecursive(t.Root, &result)
	return result
}

// insert adds a record key to the index under the given index value
func (t *bTree) insert(indexValue string, recordKey string) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	root := t.Root
	if root.isFull(t.BranchingFactor) {
		// Split root
		newRoot := &bTreeNode{
			Keys:     make([]string, 0),
			Values:   make([][]string, 0),
			Children: []*bTreeNode{root},
			IsLeaf:   false,
		}
		newRoot.splitChild(t.BranchingFactor, 0)
		t.Root = newRoot
	}
	t.Root.insertNonFull(t.BranchingFactor, indexValue, recordKey)
	t.dirty = true
	t.version++
}

// rangeSearch finds all record keys for index values in the given range
func (t *bTree) rangeSearch(min string, max string, includeMin bool, includeMax bool) []string {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	it := newBTreeIterator(t, min, max, includeMin, includeMax)
	var result []string
	for it.hasNext() {
		result = append(result, it.next())
	}
	return result
}

// bulkInsert inserts multiple key-value pairs efficiently
func (t *bTree) bulkInsert(items []bTreeItem) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Sort items by key for optimal insertion order
	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})

	for _, item := range items {
		root := t.Root
		if root.isFull(t.BranchingFactor) {
			// Split root
			newRoot := &bTreeNode{
				Keys:     make([]string, 0),
				Values:   make([][]string, 0),
				Children: []*bTreeNode{root},
				IsLeaf:   false,
			}
			newRoot.splitChild(t.BranchingFactor, 0)
			t.Root = newRoot
		}
		t.Root.insertNonFull(t.BranchingFactor, item.Key, item.Value)
	}
	t.dirty = true
	t.version++
}

// bulkDelete deletes multiple key-value pairs efficiently
func (t *bTree) bulkDelete(items []bTreeItem) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Sort items by key for optimal deletion order
	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})

	for _, item := range items {
		t.deleteRecursive(nil, t.Root, 0, item.Key, item.Value)
		// Handle root becoming empty
		if len(t.Root.Keys) == 0 && len(t.Root.Children) == 1 {
			t.Root = t.Root.Children[0]
		}
	}
	t.dirty = true
	t.version++
}

// bulkSearch performs multiple equality searches efficiently
func (t *bTree) bulkSearch(keys []string) map[string][]string {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	result := make(map[string][]string, len(keys))
	for _, key := range keys {
		result[key] = t.searchRecursive(t.Root, key)
	}
	return result
}

// search finds all record keys for a given index value
func (t *bTree) search(indexValue string) []string {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.searchRecursive(t.Root, indexValue)
}

// serialize encodes the B-tree to msgpack bytes with versioning
func (t *bTree) serialize() ([]byte, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

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
func deserializeBTree(data []byte) (*bTree, error) {
	var pb persistedBTree
	err := msgpack.Unmarshal(data, &pb)
	if err != nil {
		return nil, err
	}

	t := &bTree{
		Root:            pb.Root,
		BranchingFactor: pb.Branching,
		version:         pb.Version,
		dirty:           false, // Loaded from disk, not dirty
	}

	return t, nil
}
