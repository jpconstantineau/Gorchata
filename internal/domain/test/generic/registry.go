package generic

import (
	"sort"
	"sync"
)

// Registry manages generic test implementations
type Registry struct {
	mu    sync.RWMutex
	tests map[string]GenericTest
}

// NewRegistry creates a new empty test registry
func NewRegistry() *Registry {
	return &Registry{
		tests: make(map[string]GenericTest),
	}
}

// Register adds a test to the registry
func (r *Registry) Register(name string, test GenericTest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tests[name] = test
}

// Get retrieves a test by name
func (r *Registry) Get(name string) (GenericTest, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	test, ok := r.tests[name]
	return test, ok
}

// List returns all registered test names sorted alphabetically
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.tests))
	for name := range r.tests {
		names = append(names, name)
	}

	sort.Strings(names)
	return names
}

// NewDefaultRegistry creates a registry with all core and extended tests
func NewDefaultRegistry() *Registry {
	r := NewRegistry()

	// Core tests (DBT built-in)
	r.Register("not_null", &NotNullTest{})
	r.Register("unique", &UniqueTest{})
	r.Register("accepted_values", &AcceptedValuesTest{})
	r.Register("relationships", &RelationshipsTest{})

	// Extended tests (dbt-utils style)
	r.Register("not_empty_string", &NotEmptyStringTest{})
	r.Register("at_least_one", &AtLeastOneTest{})
	r.Register("not_constant", &NotConstantTest{})
	r.Register("unique_combination_of_columns", &UniqueCombinationTest{})
	r.Register("relationships_where", &RelationshipsWhereTest{})
	r.Register("accepted_range", &AcceptedRangeTest{})
	r.Register("recency", &RecencyTest{})
	r.Register("equal_rowcount", &EqualRowcountTest{})
	r.Register("sequential_values", &SequentialValuesTest{})
	r.Register("mutually_exclusive_ranges", &MutuallyExclusiveRangesTest{})

	return r
}
