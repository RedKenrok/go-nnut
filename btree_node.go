package nnut

import (
	"fmt"
	"slices"
	"sort"
)

// bTreeNode represents a node in the B-tree index
type bTreeNode struct {
	Keys     []string     // sorted indexed values (strings)
	Values   [][]string   // for each key, list of record keys
	Children []*bTreeNode // child nodes (len = len(Keys)+1 for internal nodes)
	IsLeaf   bool
}

// isFull returns true if the node has reached maximum capacity
func (n *bTreeNode) isFull(t int) bool {
	return len(n.Keys) >= 2*t-1
}

// splitChild splits a full child node
func (n *bTreeNode) splitChild(aCount int, bCount int) {
	aChildren := n.Children[bCount]
	bChildren := &bTreeNode{
		Keys:     make([]string, 0, 2*aCount-1),
		Values:   make([][]string, 0, 2*aCount-1),
		Children: make([]*bTreeNode, 0, 2*aCount),
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
func (n *bTreeNode) insertNonFull(t int, key string, value string) {
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

func (n *bTreeNode) isUnderfilled(t int, isRoot bool) bool {
	if isRoot && len(n.Children) == 0 {
		return false // root can be empty
	}
	return len(n.Keys) < t-1
}

func (n *bTreeNode) removeKey(i int) {
	n.Keys = slices.Delete(n.Keys, i, i+1)
	n.Values = slices.Delete(n.Values, i, i+1)
	if !n.IsLeaf {
		n.Children = slices.Delete(n.Children, i+1, i+2)
	}
}
