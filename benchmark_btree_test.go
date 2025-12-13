package nnut

import (
	"fmt"
	"testing"
)

// Benchmark B-tree operations
func BenchmarkBTreeInsert(b *testing.B) {
	bt := NewBTreeIndex(32)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("val%d", i)
		bt.Insert(key, value)
	}
}

func BenchmarkBTreeSearch(b *testing.B) {
	bt := NewBTreeIndex(32)
	// Setup
	for i := 0; i < 10000; i++ {
		bt.Insert(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%10000)
		bt.Search(key)
	}
}

func BenchmarkBTreeRange(b *testing.B) {
	bt := NewBTreeIndex(32)
	// Setup with smaller dataset
	for i := 0; i < 1000; i++ {
		bt.Insert(fmt.Sprintf("key%04d", i), fmt.Sprintf("val%d", i))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		results := bt.RangeSearch("key0500", "key0600", true, true)
		_ = results // prevent optimization
	}
}
