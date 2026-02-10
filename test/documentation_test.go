package test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDocumentationFilesExist verifies all required documentation files exist
func TestDocumentationFilesExist(t *testing.T) {
	exampleDir := filepath.Join("..", "examples", "unit_train_analytics")

	requiredFiles := []string{
		"README.md",
		"METRICS.md",
		"ARCHITECTURE.md",
	}

	for _, file := range requiredFiles {
		path := filepath.Join(exampleDir, file)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("Required documentation file does not exist: %s", file)
		}
	}
}

// TestREADMECompleteness verifies README covers all required sections
func TestREADMECompleteness(t *testing.T) {
	readmePath := filepath.Join("..", "examples", "unit_train_analytics", "README.md")

	content, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("Failed to read README.md: %v", err)
	}

	readmeContent := string(content)

	requiredSections := []string{
		"Overview",
		"Business Context",
		"Architecture",
		"Quick Start",
		"Data Generation",
		"Analytical Queries",
		"Validation",
		"Design Decisions",
		"Known Issues",
		"Future Enhancements",
	}

	for _, section := range requiredSections {
		if !strings.Contains(readmeContent, section) {
			t.Errorf("README.md is missing required section: %s", section)
		}
	}

	// Verify mentions of key concepts
	keyConcepts := []string{
		"228 cars",
		"3 trains",
		"6 corridors",
		"90 days",
		"star schema",
	}

	for _, concept := range keyConcepts {
		if !strings.Contains(readmeContent, concept) {
			t.Errorf("README.md should mention key concept: %s", concept)
		}
	}
}

// TestMetricsCatalog verifies METRICS.md documents all 7 metrics
func TestMetricsCatalog(t *testing.T) {
	metricsPath := filepath.Join("..", "examples", "unit_train_analytics", "METRICS.md")

	content, err := os.ReadFile(metricsPath)
	if err != nil {
		t.Fatalf("Failed to read METRICS.md: %v", err)
	}

	metricsContent := string(content)

	// All 7 metrics tables from Phase 6
	expectedMetrics := []string{
		"agg_corridor_weekly_metrics",
		"agg_train_daily_performance",
		"agg_car_utilization_metrics",
		"agg_straggler_analysis",
		"agg_queue_analysis",
		"agg_power_transfer_analysis",
		"agg_seasonal_performance",
	}

	for _, metric := range expectedMetrics {
		if !strings.Contains(metricsContent, metric) {
			t.Errorf("METRICS.md should document metric: %s", metric)
		}
	}

	// Verify key documentation elements
	requiredElements := []string{
		"Business Question",
		"Key Columns",
		"Usage",
	}

	for _, element := range requiredElements {
		if !strings.Contains(metricsContent, element) {
			t.Errorf("METRICS.md should contain documentation element: %s", element)
		}
	}
}

// TestArchitectureDoc verifies ARCHITECTURE.md covers all major components
func TestArchitectureDoc(t *testing.T) {
	archPath := filepath.Join("..", "examples", "unit_train_analytics", "ARCHITECTURE.md")

	content, err := os.ReadFile(archPath)
	if err != nil {
		t.Fatalf("Failed to read ARCHITECTURE.md: %v", err)
	}

	archContent := string(content)

	requiredSections := []string{
		"Data Flow",
		"Schema Design",
		"Table Relationships",
		"Performance Considerations",
		"Extensibility Points",
	}

	for _, section := range requiredSections {
		if !strings.Contains(archContent, section) {
			t.Errorf("ARCHITECTURE.md is missing required section: %s", section)
		}
	}

	// Verify mentions of key architectural concepts
	keyArchConcepts := []string{
		"star schema",
		"dimension",
		"fact",
		"grain",
	}

	for _, concept := range keyArchConcepts {
		if !strings.Contains(strings.ToLower(archContent), strings.ToLower(concept)) {
			t.Errorf("ARCHITECTURE.md should mention architectural concept: %s", concept)
		}
	}

	// Verify all table categories are mentioned
	tableCategories := []string{
		"dim_car",
		"dim_train",
		"dim_corridor",
		"dim_location",
		"dim_date",
		"fact_movement",
		"fact_straggler",
		"fact_queue",
		"fact_power_transfer",
	}

	for _, table := range tableCategories {
		if !strings.Contains(archContent, table) {
			t.Errorf("ARCHITECTURE.md should mention table: %s", table)
		}
	}
}
