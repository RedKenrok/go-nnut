package nnut

import (
	"fmt"
	"testing"
)

// Benchmark B-tree operations
func BenchmarkBTreeInsert(b *testing.B) {
	bt := newBTree(32)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("val%d", i)
		bt.insert(key, value)
	}
}

func BenchmarkBTreeSearch(b *testing.B) {
	bt := newBTree(32)
	// Setup
	for i := 0; i < 100000; i++ {
		bt.insert(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%10000)
		bt.search(key)
	}
}

func BenchmarkBTreeRangeSmall(b *testing.B) {
	bt := newBTree(32)
	for i := 0; i < 100000; i++ {
		bt.insert(fmt.Sprintf("key%04d", i), fmt.Sprintf("val%d", i))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		results := bt.rangeSearch("key0500", "key0501", true, true)
		_ = results // prevent optimization
	}
}

func BenchmarkBTreeRangeMedium(b *testing.B) {
	bt := newBTree(32)
	for i := 0; i < 100000; i++ {
		bt.insert(fmt.Sprintf("key%04d", i), fmt.Sprintf("val%d", i))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		results := bt.rangeSearch("key0500", "key0600", true, true)
		_ = results // prevent optimization
	}
}

func BenchmarkBTreeRangeLarge(b *testing.B) {
	bt := newBTree(32)
	for i := 0; i < 100000; i++ {
		bt.insert(fmt.Sprintf("key%04d", i), fmt.Sprintf("val%d", i))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		results := bt.rangeSearch("key0100", "key0900", true, true)
		_ = results // prevent optimization
	}
}

func BenchmarkBTreeRangeManyValues(b *testing.B) {
	bt := newBTree(32)
	// Insert 10000 keys, each with 10 values
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%04d", i)
		for j := 0; j < 10; j++ {
			value := fmt.Sprintf("val%d_%d", i, j)
			bt.insert(key, value)
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		results := bt.rangeSearch("key0500", "key0600", true, true)
		_ = results // prevent optimization
	}
}

func BenchmarkBTreeRangeDeepTree(b *testing.B) {
	bt := newBTree(4) // Smaller branching factor for deeper tree
	for i := 0; i < 100000; i++ {
		bt.insert(fmt.Sprintf("key%04d", i), fmt.Sprintf("val%d", i))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		results := bt.rangeSearch("key0500", "key0600", true, true)
		_ = results // prevent optimization
	}
}

func BenchmarkBTreeIterator(b *testing.B) {
	bt := newBTree(32)
	for i := 0; i < 100000; i++ {
		bt.insert(fmt.Sprintf("key%04d", i), fmt.Sprintf("val%d", i))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		it := newBTreeIterator(bt, "key0500", "key0600", true, true)
		for it.hasNext() {
			_ = it.next()
		}
	}
}
