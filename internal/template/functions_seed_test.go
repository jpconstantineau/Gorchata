package template

import (
	"testing"
)

// TestSeedFunc_ExistingSeed verifies seed() returns qualified table name for valid seed
func TestSeedFunc_ExistingSeed(t *testing.T) {
	ctx := NewContext()
	ctx.Seeds = map[string]string{
		"customers": "customers",
	}

	seedFunc := makeSeedFunc(ctx)
	result, err := seedFunc("customers")

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := "customers"
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

// TestSeedFunc_WithSchema verifies seed() includes schema prefix when configured
func TestSeedFunc_WithSchema(t *testing.T) {
	ctx := NewContext()
	ctx.Seeds = map[string]string{
		"customers": "staging.customers",
	}

	seedFunc := makeSeedFunc(ctx)
	result, err := seedFunc("customers")

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := "staging.customers"
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

// TestSeedFunc_NonexistentSeed verifies seed() returns error for unknown seed
func TestSeedFunc_NonexistentSeed(t *testing.T) {
	ctx := NewContext()
	ctx.Seeds = map[string]string{
		"customers": "customers",
	}

	seedFunc := makeSeedFunc(ctx)
	_, err := seedFunc("nonexistent")

	if err == nil {
		t.Fatal("expected error for nonexistent seed, got nil")
	}

	expectedMsg := "seed \"nonexistent\" not found"
	if err.Error() != expectedMsg {
		t.Errorf("expected error message %q, got %q", expectedMsg, err.Error())
	}
}

// TestSeedFunc_EmptyName verifies seed() handles empty seed name
func TestSeedFunc_EmptyName(t *testing.T) {
	ctx := NewContext()
	ctx.Seeds = map[string]string{
		"customers": "customers",
	}

	seedFunc := makeSeedFunc(ctx)
	_, err := seedFunc("")

	if err == nil {
		t.Fatal("expected error for empty seed name, got nil")
	}

	expectedMsg := "seed name cannot be empty"
	if err.Error() != expectedMsg {
		t.Errorf("expected error message %q, got %q", expectedMsg, err.Error())
	}
}

// TestSeedFunc_MultipleCalls verifies seed() handles multiple seed references in one template
func TestSeedFunc_MultipleCalls(t *testing.T) {
	ctx := NewContext()
	ctx.Seeds = map[string]string{
		"customers": "customers",
		"orders":    "staging.orders",
		"products":  "products",
	}

	seedFunc := makeSeedFunc(ctx)

	// Call for first seed
	result1, err1 := seedFunc("customers")
	if err1 != nil {
		t.Fatalf("expected no error for customers, got %v", err1)
	}
	if result1 != "customers" {
		t.Errorf("expected %q, got %q", "customers", result1)
	}

	// Call for second seed
	result2, err2 := seedFunc("orders")
	if err2 != nil {
		t.Fatalf("expected no error for orders, got %v", err2)
	}
	if result2 != "staging.orders" {
		t.Errorf("expected %q, got %q", "staging.orders", result2)
	}

	// Call for third seed
	result3, err3 := seedFunc("products")
	if err3 != nil {
		t.Fatalf("expected no error for products, got %v", err3)
	}
	if result3 != "products" {
		t.Errorf("expected %q, got %q", "products", result3)
	}
}
