package nnut

import "sort"

type iteratorNode struct {
	node  *bTreeNode
	index int
}

// bTreeIterator provides efficient iteration over B-tree range queries
type bTreeIterator struct {
	tree          *bTree
	min, max      string
	includeMin    bool
	includeMax    bool
	path          []iteratorNode
	currentValues []string
	valueIndex    int
	finished      bool
}

// newBTreeIterator creates a new iterator for range queries
func newBTreeIterator(tree *bTree, min, max string, includeMin, includeMax bool) *bTreeIterator {
	iterator := &bTreeIterator{
		tree:       tree,
		min:        min,
		max:        max,
		includeMin: includeMin,
		includeMax: includeMax,
		path:       make([]iteratorNode, 0),
		finished:   false,
	}
	iterator.findStart()
	iterator.advance()
	return iterator
}

// findStart builds the path to the starting position for the range
func (it *bTreeIterator) findStart() {
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
func (it *bTreeIterator) advance() {
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
func (it *bTreeIterator) isInRange(key string) bool {
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
func (it *bTreeIterator) isKeyGreaterThanMax(key string) bool {
	if it.max == "" {
		return false
	}
	if it.includeMax {
		return key > it.max
	}
	return key >= it.max
}

// isSubtreeLessThanMin checks if an entire subtree is less than the min bound
func (it *bTreeIterator) isSubtreeLessThanMin(node *bTreeNode, childIndex int) bool {
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
func (it *bTreeIterator) isSubtreeGreaterThanMax(node *bTreeNode, childIndex int) bool {
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

// hasNext returns true if there are more values to iterate
func (it *bTreeIterator) hasNext() bool {
	return !it.finished && it.valueIndex < len(it.currentValues)
}

// next returns the next value in the iteration
func (it *bTreeIterator) next() string {
	if !it.hasNext() {
		return ""
	}
	value := it.currentValues[it.valueIndex]
	it.valueIndex++
	if it.valueIndex >= len(it.currentValues) {
		it.advance()
	}
	return value
}
