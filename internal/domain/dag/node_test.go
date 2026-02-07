package dag

import (
	"testing"
)

func TestNodeCreation(t *testing.T) {
	node := &Node{
		ID:   "model_users",
		Name: "users",
		Type: "model",
	}

	if node.ID != "model_users" {
		t.Errorf("expected ID 'model_users', got '%s'", node.ID)
	}
	if node.Name != "users" {
		t.Errorf("expected Name 'users', got '%s'", node.Name)
	}
	if node.Type != "model" {
		t.Errorf("expected Type 'model', got '%s'", node.Type)
	}
	if node.Dependencies != nil {
		t.Errorf("expected nil Dependencies, got %v", node.Dependencies)
	}
}

func TestNodeWithDependencies(t *testing.T) {
	node := &Node{
		ID:           "model_orders",
		Name:         "orders",
		Type:         "model",
		Dependencies: []string{"model_users", "model_products"},
	}

	if len(node.Dependencies) != 2 {
		t.Errorf("expected 2 dependencies, got %d", len(node.Dependencies))
	}
	if node.Dependencies[0] != "model_users" {
		t.Errorf("expected first dependency 'model_users', got '%s'", node.Dependencies[0])
	}
	if node.Dependencies[1] != "model_products" {
		t.Errorf("expected second dependency 'model_products', got '%s'", node.Dependencies[1])
	}
}

func TestNodeMetadata(t *testing.T) {
	metadata := map[string]interface{}{
		"file_path": "/path/to/model.sql",
		"content":   "SELECT * FROM users",
		"size":      1024,
	}

	node := &Node{
		ID:       "model_users",
		Name:     "users",
		Type:     "model",
		Metadata: metadata,
	}

	if node.Metadata == nil {
		t.Fatal("expected Metadata to be set, got nil")
	}

	filePath, ok := node.Metadata["file_path"].(string)
	if !ok || filePath != "/path/to/model.sql" {
		t.Errorf("expected file_path '/path/to/model.sql', got '%v'", filePath)
	}

	content, ok := node.Metadata["content"].(string)
	if !ok || content != "SELECT * FROM users" {
		t.Errorf("expected content 'SELECT * FROM users', got '%v'", content)
	}

	size, ok := node.Metadata["size"].(int)
	if !ok || size != 1024 {
		t.Errorf("expected size 1024, got %v", size)
	}
}
