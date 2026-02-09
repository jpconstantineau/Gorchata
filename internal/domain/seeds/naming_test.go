package seeds

import (
	"path/filepath"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/config"
)

func TestResolveTableName_Filename(t *testing.T) {
	tests := []struct {
		name         string
		seedFilePath string
		namingConfig *config.NamingConfig
		expected     string
	}{
		{
			name:         "simple filename",
			seedFilePath: "seeds/customers.csv",
			namingConfig: &config.NamingConfig{
				Strategy: config.NamingStrategyFilename,
			},
			expected: "customers",
		},
		{
			name:         "filename in nested path",
			seedFilePath: "data/seeds/prod/orders.csv",
			namingConfig: &config.NamingConfig{
				Strategy: config.NamingStrategyFilename,
			},
			expected: "orders",
		},
		{
			name:         "filename with underscores",
			seedFilePath: "seeds/customer_orders.csv",
			namingConfig: &config.NamingConfig{
				Strategy: config.NamingStrategyFilename,
			},
			expected: "customer_orders",
		},
		{
			name:         "absolute path",
			seedFilePath: "/var/data/seeds/products.csv",
			namingConfig: &config.NamingConfig{
				Strategy: config.NamingStrategyFilename,
			},
			expected: "products",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ResolveTableName(tt.seedFilePath, tt.namingConfig)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestResolveTableName_Folder(t *testing.T) {
	tests := []struct {
		name         string
		seedFilePath string
		namingConfig *config.NamingConfig
		expected     string
	}{
		{
			name:         "parent folder name",
			seedFilePath: "seeds/sales/data.csv",
			namingConfig: &config.NamingConfig{
				Strategy: config.NamingStrategyFolder,
			},
			expected: "sales",
		},
		{
			name:         "nested folder",
			seedFilePath: "data/seeds/marketing/campaigns/file.csv",
			namingConfig: &config.NamingConfig{
				Strategy: config.NamingStrategyFolder,
			},
			expected: "campaigns",
		},
		{
			name:         "folder with underscores",
			seedFilePath: "seeds/customer_data/export.csv",
			namingConfig: &config.NamingConfig{
				Strategy: config.NamingStrategyFolder,
			},
			expected: "customer_data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ResolveTableName(tt.seedFilePath, tt.namingConfig)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestResolveTableName_Static(t *testing.T) {
	tests := []struct {
		name         string
		seedFilePath string
		namingConfig *config.NamingConfig
		expected     string
	}{
		{
			name:         "static name",
			seedFilePath: "seeds/anything.csv",
			namingConfig: &config.NamingConfig{
				Strategy:   config.NamingStrategyStatic,
				StaticName: "my_custom_table",
			},
			expected: "my_custom_table",
		},
		{
			name:         "static name overrides path",
			seedFilePath: "data/seeds/folder/file.csv",
			namingConfig: &config.NamingConfig{
				Strategy:   config.NamingStrategyStatic,
				StaticName: "import_data",
			},
			expected: "import_data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ResolveTableName(tt.seedFilePath, tt.namingConfig)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestResolveTableName_WithPrefix(t *testing.T) {
	tests := []struct {
		name         string
		seedFilePath string
		namingConfig *config.NamingConfig
		expected     string
	}{
		{
			name:         "filename with prefix",
			seedFilePath: "seeds/customers.csv",
			namingConfig: &config.NamingConfig{
				Strategy: config.NamingStrategyFilename,
				Prefix:   "seed_",
			},
			expected: "seed_customers",
		},
		{
			name:         "folder with prefix",
			seedFilePath: "seeds/sales/data.csv",
			namingConfig: &config.NamingConfig{
				Strategy: config.NamingStrategyFolder,
				Prefix:   "import_",
			},
			expected: "import_sales",
		},
		{
			name:         "static with prefix",
			seedFilePath: "seeds/anything.csv",
			namingConfig: &config.NamingConfig{
				Strategy:   config.NamingStrategyStatic,
				StaticName: "custom_table",
				Prefix:     "raw_",
			},
			expected: "raw_custom_table",
		},
		{
			name:         "empty prefix (no effect)",
			seedFilePath: "seeds/products.csv",
			namingConfig: &config.NamingConfig{
				Strategy: config.NamingStrategyFilename,
				Prefix:   "",
			},
			expected: "products",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ResolveTableName(tt.seedFilePath, tt.namingConfig)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestResolveTableName_Precedence(t *testing.T) {
	// Static strategy should always use static_name, even if filepath changes
	namingConfig := &config.NamingConfig{
		Strategy:   config.NamingStrategyStatic,
		StaticName: "fixed_table",
	}

	testPaths := []string{
		"seeds/customers.csv",
		"data/sales/orders.csv",
		"different/folder/file.csv",
	}

	for _, path := range testPaths {
		result := ResolveTableName(path, namingConfig)
		if result != "fixed_table" {
			t.Errorf("static strategy should always return %q for path %q, got %q",
				"fixed_table", path, result)
		}
	}
}

func TestResolveTableName_EdgeCases(t *testing.T) {
	tests := []struct {
		name         string
		seedFilePath string
		namingConfig *config.NamingConfig
		expected     string
	}{
		{
			name:         "file without extension",
			seedFilePath: "seeds/customers",
			namingConfig: &config.NamingConfig{
				Strategy: config.NamingStrategyFilename,
			},
			expected: "customers",
		},
		{
			name:         "multiple dots in filename",
			seedFilePath: "seeds/data.backup.csv",
			namingConfig: &config.NamingConfig{
				Strategy: config.NamingStrategyFilename,
			},
			expected: "data.backup",
		},
		{
			name:         "windows path separators",
			seedFilePath: "seeds\\subfolder\\products.csv",
			namingConfig: &config.NamingConfig{
				Strategy: config.NamingStrategyFolder,
			},
			expected: "subfolder",
		},
		{
			name:         "single file in root",
			seedFilePath: "customers.csv",
			namingConfig: &config.NamingConfig{
				Strategy: config.NamingStrategyFilename,
			},
			expected: "customers",
		},
		{
			name:         "deeply nested path",
			seedFilePath: "a/b/c/d/e/f/data.csv",
			namingConfig: &config.NamingConfig{
				Strategy: config.NamingStrategyFolder,
			},
			expected: "f",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ResolveTableName(tt.seedFilePath, tt.namingConfig)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

// Helper test to ensure we handle cross-platform paths correctly
func TestResolveTableName_CrossPlatform(t *testing.T) {
	// Use filepath.Join to create platform-specific paths
	unixStyle := "seeds/subfolder/data.csv"
	windowsStyle := filepath.Join("seeds", "subfolder", "data.csv")

	config := &config.NamingConfig{
		Strategy: config.NamingStrategyFilename,
	}

	result1 := ResolveTableName(unixStyle, config)
	result2 := ResolveTableName(windowsStyle, config)

	// Both should resolve to "data"
	if result1 != "data" {
		t.Errorf("unix-style path: expected %q, got %q", "data", result1)
	}
	if result2 != "data" {
		t.Errorf("windows-style path: expected %q, got %q", "data", result2)
	}
}
