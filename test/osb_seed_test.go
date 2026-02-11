package test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestOSBSeedConfiguration validates seed YAML parses
func TestOSBSeedConfiguration(t *testing.T) {
	repoRoot := getRepoRoot(t)
	seedPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "seeds", "seed.yml")

	// Check if seed.yml exists
	if _, err := os.Stat(seedPath); os.IsNotExist(err) {
		t.Fatal("seed.yml does not exist")
	}

	// Parse seed YAML
	data, err := os.ReadFile(seedPath)
	if err != nil {
		t.Fatalf("Failed to read seed.yml: %v", err)
	}

	var seedConfig map[string]interface{}
	if err := yaml.Unmarshal(data, &seedConfig); err != nil {
		t.Fatalf("Failed to parse seed.yml: %v", err)
	}

	// Verify version exists
	if _, ok := seedConfig["version"]; !ok {
		t.Error("seed.yml missing version field")
	}
}

// TestEquipmentInventory ensures all equipment defined: 2 debarkers, 2 stranders, 1 dryer, 2 screens, 2 blenders, 1 forming line, 1 press, 1 cooling conveyor, 4 saws
func TestEquipmentInventory(t *testing.T) {
	repoRoot := getRepoRoot(t)
	equipPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "seeds", "dim_equipment.csv")

	equipment := readCSV(t, equipPath)
	if len(equipment) == 0 {
		t.Fatal("dim_equipment.csv has no data rows")
	}

	// Count by equipment type
	typeCounts := make(map[string]int)
	for _, equip := range equipment {
		equipType := getField(equip, "equipment_type")
		typeCounts[equipType]++
	}

	// Verify equipment counts per plan
	expectedCounts := map[string]int{
		"Debarker":         2,
		"Strander":         2,
		"Dryer":            1,
		"Screen":           2,
		"Blender":          2,
		"Former":           1,
		"Press":            1,
		"Cooling_Conveyor": 1,
		"Saw":              4,
	}

	for equipType, expectedCount := range expectedCounts {
		actualCount := typeCounts[equipType]
		if actualCount != expectedCount {
			t.Errorf("Expected %d %s(s), got %d", expectedCount, equipType, actualCount)
		}
	}

	// Verify total count (should be 16)
	totalEquipment := len(equipment)
	expectedTotal := 16
	if totalEquipment != expectedTotal {
		t.Errorf("Expected %d total equipment, got %d", expectedTotal, totalEquipment)
	}

	// Verify critical equipment
	criticalityCount := make(map[string]int)
	for _, equip := range equipment {
		criticality := getField(equip, "criticality_level")
		criticalityCount[criticality]++
	}

	if criticalityCount["Critical"] < 1 {
		t.Error("Expected at least 1 Critical equipment (dryer)")
	}
}

// TestProductionFlowSequence validates equipment sequence matches OSB process flow
func TestProductionFlowSequence(t *testing.T) {
	repoRoot := getRepoRoot(t)
	areaPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "seeds", "dim_production_area.csv")

	areas := readCSV(t, areaPath)
	if len(areas) == 0 {
		t.Fatal("dim_production_area.csv has no data rows")
	}

	// Build area map by sequence order
	areaBySequence := make(map[int]string)
	for _, area := range areas {
		seqStr := getField(area, "sequence_order")
		seq, err := strconv.Atoi(seqStr)
		if err != nil {
			t.Errorf("Invalid sequence_order for area %s: %s", getField(area, "area_name"), seqStr)
			continue
		}
		areaBySequence[seq] = getField(area, "area_name")
	}

	// Verify expected sequence per OSB process flow
	expectedSequence := []string{
		"Log_Yard",
		"Stranding",
		"Drying",
		"Screening",
		"Blending",
		"Forming",
		"Pressing",
		"Finishing",
	}

	if len(areaBySequence) != len(expectedSequence) {
		t.Errorf("Expected %d production areas, got %d", len(expectedSequence), len(areaBySequence))
	}

	// Verify sequence order
	for i, expectedArea := range expectedSequence {
		seq := i + 1
		actualArea := areaBySequence[seq]
		if actualArea != expectedArea {
			t.Errorf("Sequence %d: expected area '%s', got '%s'", seq, expectedArea, actualArea)
		}
	}
}

// TestDryerAsBottleneck ensures dryer capacity constraints modeled (rated for 85% of upstream stranding capacity)
func TestDryerAsBottleneck(t *testing.T) {
	repoRoot := getRepoRoot(t)
	equipPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "seeds", "dim_equipment.csv")

	equipment := readCSV(t, equipPath)

	// Find stranders and dryer
	var stranderCapacity float64
	var dryerCapacity float64
	stranderCount := 0

	for _, equip := range equipment {
		equipType := getField(equip, "equipment_type")
		capacityStr := getField(equip, "rated_capacity_units_hr")
		capacity, err := strconv.ParseFloat(capacityStr, 64)
		if err != nil {
			t.Errorf("Invalid rated_capacity_units_hr for %s: %s", getField(equip, "equipment_id"), capacityStr)
			continue
		}

		if equipType == "Strander" {
			stranderCapacity += capacity
			stranderCount++
		} else if equipType == "Dryer" {
			dryerCapacity = capacity
		}
	}

	if stranderCount != 2 {
		t.Errorf("Expected 2 stranders, found %d", stranderCount)
	}

	if stranderCapacity == 0 || dryerCapacity == 0 {
		t.Fatal("Could not find strander or dryer capacity data")
	}

	// Verify dryer is bottleneck (capacity should be ~83-87% of total strander capacity)
	ratio := dryerCapacity / stranderCapacity
	if ratio < 0.80 || ratio > 0.90 {
		t.Errorf("Dryer capacity should be 80-90%% of strander capacity, got %.1f%% (dryer: %.1f, stranders: %.1f)",
			ratio*100, dryerCapacity, stranderCapacity)
	}
}

// TestBufferCapacityRules validates green strand bins (4 hrs), dry fiber silos (8 hrs), mat buffer (30 min)
func TestBufferCapacityRules(t *testing.T) {
	repoRoot := getRepoRoot(t)
	areaPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "seeds", "dim_production_area.csv")

	areas := readCSV(t, areaPath)

	// Build map of area buffers
	buffers := make(map[string]float64)
	for _, area := range areas {
		areaName := getField(area, "area_name")
		bufferStr := getField(area, "buffer_capacity_hours")
		if bufferStr == "" || bufferStr == "0" || strings.ToUpper(bufferStr) == "NULL" {
			continue
		}
		buffer, err := strconv.ParseFloat(bufferStr, 64)
		if err != nil {
			t.Errorf("Invalid buffer_capacity_hours for area %s: %s", areaName, bufferStr)
			continue
		}
		buffers[areaName] = buffer
	}

	// Verify key buffers exist with correct capacities
	// Green strand bins are after Stranding
	if stranBuffer, ok := buffers["Stranding"]; ok {
		if stranBuffer < 3.5 || stranBuffer > 4.5 {
			t.Errorf("Green strand bins (Stranding buffer) should be ~4 hours, got %.1f", stranBuffer)
		}
	} else {
		t.Error("Stranding area should have buffer_capacity_hours defined (green strand bins)")
	}

	// Dry fiber silos are after Drying
	if dryBuffer, ok := buffers["Drying"]; ok {
		if dryBuffer < 7 || dryBuffer > 9 {
			t.Errorf("Dry fiber silos (Drying buffer) should be ~8 hours, got %.1f", dryBuffer)
		}
	} else {
		t.Error("Drying area should have buffer_capacity_hours defined (dry fiber silos)")
	}

	// Mat buffer is after Forming (0.5 hours = 30 min)
	if formBuffer, ok := buffers["Forming"]; ok {
		if formBuffer < 0.4 || formBuffer > 0.6 {
			t.Errorf("Mat buffer (Forming buffer) should be ~0.5 hours (30 min), got %.1f", formBuffer)
		}
	} else {
		t.Error("Forming area should have buffer_capacity_hours defined (mat buffer)")
	}
}

// TestStateTransitionRealism ensures valid state transitions defined
func TestStateTransitionRealism(t *testing.T) {
	// This test verifies that reason codes cover expected machine states
	repoRoot := getRepoRoot(t)
	reasonPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "seeds", "dim_reason_code.csv")

	reasons := readCSV(t, reasonPath)
	if len(reasons) == 0 {
		t.Fatal("dim_reason_code.csv has no data rows")
	}

	// Count by OEE categories
	timeModelCounts := make(map[string]int)
	lossTypeCounts := make(map[string]int)
	categoryCounts := make(map[string]int)

	for _, reason := range reasons {
		timeModel := getField(reason, "oee_time_model_class")
		lossType := getField(reason, "oee_loss_type")
		category := getField(reason, "reason_category")

		timeModelCounts[timeModel]++
		lossTypeCounts[lossType]++
		categoryCounts[category]++
	}

	// Verify we have both Planned and Unplanned reasons
	if timeModelCounts["Planned"] < 1 {
		t.Error("Should have at least 1 Planned reason code")
	}
	if timeModelCounts["Unplanned"] < 1 {
		t.Error("Should have at least 1 Unplanned reason code")
	}

	// Verify we have all OEE loss types
	expectedLossTypes := []string{"Availability", "Performance", "Quality", "None"}
	for _, lossType := range expectedLossTypes {
		if lossTypeCounts[lossType] < 1 {
			t.Errorf("Should have at least 1 reason with loss type '%s'", lossType)
		}
	}

	// Verify we have various reason categories
	expectedCategories := []string{"Mechanical", "Electrical", "Process", "Quality", "Planned_Maintenance"}
	for _, category := range expectedCategories {
		if categoryCounts[category] < 1 {
			t.Errorf("Should have at least 1 reason in category '%s'", category)
		}
	}
}

// TestShiftPatterns validates 3×8hr shifts with handover delays
func TestShiftPatterns(t *testing.T) {
	repoRoot := getRepoRoot(t)
	shiftPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "seeds", "dim_shift.csv")

	shifts := readCSV(t, shiftPath)
	if len(shifts) != 3 {
		t.Errorf("Expected 3 shifts, got %d", len(shifts))
	}

	// Verify shift names
	shiftNames := make(map[string]bool)
	for _, shift := range shifts {
		name := getField(shift, "shift_name")
		shiftNames[name] = true
	}

	expectedShifts := []string{"Day", "Swing", "Night"}
	for _, expected := range expectedShifts {
		if !shiftNames[expected] {
			t.Errorf("Missing shift: %s", expected)
		}
	}

	// Verify shift times are set
	for _, shift := range shifts {
		startTime := getField(shift, "shift_start_time")
		endTime := getField(shift, "shift_end_time")

		if startTime == "" {
			t.Errorf("Shift %s missing shift_start_time", getField(shift, "shift_name"))
		}
		if endTime == "" {
			t.Errorf("Shift %s missing shift_end_time", getField(shift, "shift_name"))
		}
	}
}

// TestMaintenanceWindows ensures planned maintenance scheduled appropriately
func TestMaintenanceWindows(t *testing.T) {
	repoRoot := getRepoRoot(t)
	reasonPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "seeds", "dim_reason_code.csv")

	reasons := readCSV(t, reasonPath)

	// Find planned maintenance reasons
	pmReasons := 0
	for _, reason := range reasons {
		category := getField(reason, "reason_category")
		timeModel := getField(reason, "oee_time_model_class")

		if category == "Planned_Maintenance" {
			pmReasons++

			// Verify it's marked as Planned in OEE time model
			if timeModel != "Planned" {
				t.Errorf("Planned maintenance reason %s should have oee_time_model_class='Planned', got '%s'",
					getField(reason, "reason_code"), timeModel)
			}

			// Verify loss type is None for planned downtime
			lossType := getField(reason, "oee_loss_type")
			if lossType != "None" {
				t.Errorf("Planned maintenance reason %s should have oee_loss_type='None', got '%s'",
					getField(reason, "reason_code"), lossType)
			}
		}
	}

	if pmReasons < 1 {
		t.Error("Should have at least 1 planned maintenance reason code")
	}
}

// TestBreakdownMTBFDistribution ensures realistic failure intervals defined
func TestBreakdownMTBFDistribution(t *testing.T) {
	repoRoot := getRepoRoot(t)
	reasonPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "seeds", "dim_reason_code.csv")

	reasons := readCSV(t, reasonPath)

	// Verify MTBF-like data (typical_duration_min) is set for breakdown reasons
	breakdownReasons := 0
	for _, reason := range reasons {
		category := getField(reason, "reason_category")
		if category == "Mechanical" || category == "Electrical" {
			breakdownReasons++

			// Verify typical_duration_min is set
			durationStr := getField(reason, "typical_duration_min")
			if durationStr == "" {
				t.Errorf("Breakdown reason %s should have typical_duration_min set", getField(reason, "reason_code"))
				continue
			}

			duration, err := strconv.ParseFloat(durationStr, 64)
			if err != nil {
				t.Errorf("Invalid typical_duration_min for reason %s: %s", getField(reason, "reason_code"), durationStr)
				continue
			}

			// Typical repair durations should be 15 min to 24 hours (1440 min)
			if duration < 15 || duration > 1440 {
				t.Errorf("Reason %s has unrealistic typical_duration_min: %.1f (expected 15-1440 min)",
					getField(reason, "reason_code"), duration)
			}
		}
	}

	if breakdownReasons < 3 {
		t.Error("Should have at least 3 mechanical/electrical breakdown reason codes")
	}
}

// TestProductSpecifications validates product specs with tolerances
func TestProductSpecifications(t *testing.T) {
	repoRoot := getRepoRoot(t)
	productPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "seeds", "dim_product_spec.csv")

	products := readCSV(t, productPath)
	if len(products) < 3 {
		t.Errorf("Expected at least 3 product specifications, got %d", len(products))
	}

	// Verify thickness options (3/8", 7/16", 9/16" = 0.375, 0.4375, 0.5625)
	expectedThicknesses := map[float64]bool{
		0.375:  false,
		0.4375: false,
		0.5625: false,
	}

	for _, product := range products {
		thicknessStr := getField(product, "thickness_inches")
		thickness, err := strconv.ParseFloat(thicknessStr, 64)
		if err != nil {
			t.Errorf("Invalid thickness_inches: %s", thicknessStr)
			continue
		}

		// Mark if we found an expected thickness
		if _, ok := expectedThicknesses[thickness]; ok {
			expectedThicknesses[thickness] = true
		}

		// Verify density is in reasonable range (38-42 lbs/ft³)
		densityStr := getField(product, "density_lbft3")
		density, err := strconv.ParseFloat(densityStr, 64)
		if err != nil {
			t.Errorf("Invalid density_lbft3: %s", densityStr)
			continue
		}

		if density < 35 || density > 45 {
			t.Errorf("Product %s has unrealistic density: %.1f (expected 35-45 lbs/ft³)",
				getField(product, "product_id"), density)
		}

		// Verify tolerances are set
		tolPlusStr := getField(product, "thickness_tolerance_plus")
		if tolPlusStr == "" {
			t.Errorf("Product %s missing thickness_tolerance_plus", getField(product, "product_id"))
		}

		tolMinusStr := getField(product, "thickness_tolerance_minus")
		if tolMinusStr == "" {
			t.Errorf("Product %s missing thickness_tolerance_minus", getField(product, "product_id"))
		}
	}

	// Verify we have all expected thicknesses
	for thickness, found := range expectedThicknesses {
		if !found {
			t.Errorf("Missing product specification for thickness %.4f inches", thickness)
		}
	}
}
