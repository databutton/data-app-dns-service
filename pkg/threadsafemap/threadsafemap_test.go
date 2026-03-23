package threadsafemap

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestThreadSafeMap(t *testing.T) {
	// Create a new thread-safe map
	tsm := New[string, int]()
	defer tsm.Close()

	// Create from existing map
	existing := map[string]int{
		"apple":  5,
		"banana": 3,
		"cherry": 8,
	}
	tsm2 := NewThreadSafeMapFromMap(existing)
	defer tsm2.Close()

	// Demonstrate concurrent usage
	var wg sync.WaitGroup

	// Start multiple goroutines writing to the map
	for i := range 5 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range 10 {
				key := fmt.Sprintf("key_%d_%d", id, j)
				tsm.SetAsync(key, id*10+j)
			}
		}(i)
	}

	// Start multiple goroutines reading from the map
	for i := range 3 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range 20 {
				key := fmt.Sprintf("key_%d_%d", j%5, j%10)
				if value, ok := tsm.Get(key); ok {
					fmt.Printf("Reader %d found %s: %d\n", id, key, value)
				}
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Test the map initialized from existing data
	if value, ok := tsm2.Get("apple"); ok {
		fmt.Printf("Found apple: %d\n", value)
	}

	// Add some data to the second map
	tsm2.SetSync("orange", 12)
	if value, ok := tsm2.Get("orange"); ok {
		fmt.Printf("Found orange: %d\n", value)
	} else {
		t.Log("orange not found")
		t.Fail()
	}

	t0 := time.Now()
	for i := range 100 * 1000 {
		tsm.SetAsync(fmt.Sprintf("key_%d", i), i)
	}
	t1 := time.Now()
	fmt.Printf("Set took %v\n", t1.Sub(t0)) // 32 ms

	wg.Wait()

	fmt.Printf("Final map size: %d\n", tsm.Len())
	fmt.Printf("Map 2 size: %d\n", tsm2.Len())
	fmt.Printf("Map 2 keys: %v\n", tsm2.Keys())
}
