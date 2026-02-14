package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Asset represents a sensor tag from dim_asset
type Asset struct {
	TagID         string
	EquipmentName string
	UnitName      string
}

// SensorReading represents a single sensor measurement
type SensorReading struct {
	ReadingID       int64
	Timestamp       time.Time
	TagID           string
	ParameterType   string
	MeasuredValue   float64
	DataQualityFlag string
}

// ParameterSpec defines characteristics of a parameter type
type ParameterSpec struct {
	Type      string
	MinNormal float64
	MaxNormal float64
	MinLimit  float64
	MaxLimit  float64
}

var parameterSpecs = map[string]ParameterSpec{
	"Pressure": {
		Type:      "Pressure",
		MinNormal: 50.0,
		MaxNormal: 750.0,
		MinLimit:  0.0,
		MaxLimit:  3000.0,
	},
	"Temperature": {
		Type:      "Temperature",
		MinNormal: 300.0,
		MaxNormal: 950.0,
		MinLimit:  32.0,
		MaxLimit:  1400.0,
	},
	"pH": {
		Type:      "pH",
		MinNormal: 5.0,
		MaxNormal: 9.0,
		MinLimit:  0.0,
		MaxLimit:  14.0,
	},
	"Flow": {
		Type:      "Flow",
		MinNormal: 5000.0,
		MaxNormal: 85000.0,
		MinLimit:  0.0,
		MaxLimit:  50000.0,
	},
}

// determineSensorTypes determines which sensors an asset has based on equipment type
func determineSensorTypes(equipmentName string) []string {
	equipmentNameLower := strings.ToLower(equipmentName)
	sensorMap := make(map[string]bool)

	// Pumps: Pressure + Flow (check first, most specific)
	if strings.Contains(equipmentNameLower, "pump") {
		sensorMap["Pressure"] = true
		sensorMap["Flow"] = true
		return mapKeysToSlice(sensorMap)
	}

	// Compressors, Blowers: Pressure
	if strings.Contains(equipmentNameLower, "compressor") ||
		strings.Contains(equipmentNameLower, "blower") {
		sensorMap["Pressure"] = true
		return mapKeysToSlice(sensorMap)
	}

	// Furnaces, Heaters: Temperature only
	if strings.Contains(equipmentNameLower, "furnace") {
		sensorMap["Temperature"] = true
		return mapKeysToSlice(sensorMap)
	}

	// Heat Exchangers, Coolers: Temperature (inlet/outlet)
	if strings.Contains(equipmentNameLower, "exchanger") ||
		strings.Contains(equipmentNameLower, "cooler") ||
		(strings.Contains(equipmentNameLower, "heater") && !strings.Contains(equipmentNameLower, "furnace")) {
		sensorMap["Temperature"] = true
	}

	// Columns, Drums, Towers: Temperature + Pressure
	if strings.Contains(equipmentNameLower, "column") ||
		strings.Contains(equipmentNameLower, "drum") ||
		strings.Contains(equipmentNameLower, "tower") ||
		strings.Contains(equipmentNameLower, "vessel") ||
		strings.Contains(equipmentNameLower, "reactor") ||
		strings.Contains(equipmentNameLower, "stripper") ||
		strings.Contains(equipmentNameLower, "absorber") ||
		strings.Contains(equipmentNameLower, "contactor") ||
		strings.Contains(equipmentNameLower, "separator") ||
		strings.Contains(equipmentNameLower, "stabilizer") {
		sensorMap["Temperature"] = true
		sensorMap["Pressure"] = true
	}

	// Vessels handling corrosive streams: add pH
	if strings.Contains(equipmentNameLower, "amine") ||
		strings.Contains(equipmentNameLower, "sour water") ||
		strings.Contains(equipmentNameLower, "wash") {
		sensorMap["pH"] = true
	}

	// Default: if no sensors assigned, give it Temperature
	if len(sensorMap) == 0 {
		sensorMap["Temperature"] = true
	}

	return mapKeysToSlice(sensorMap)
}

// mapKeysToSlice converts map keys to a sorted slice for consistency
func mapKeysToSlice(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// generateBaseValue generates a base value for a sensor reading
func generateBaseValue(paramType string, timestamp time.Time, seed int64) float64 {
	spec := parameterSpecs[paramType]
	rng := rand.New(rand.NewSource(seed + timestamp.Unix()))

	// Base value in normal range
	baseValue := spec.MinNormal + (spec.MaxNormal-spec.MinNormal)*rng.Float64()

	// Add daily cycle (12-hour period for refinery operations)
	hourOfDay := float64(timestamp.Hour())
	dailyCycle := 0.1 * (spec.MaxNormal - spec.MinNormal) * math.Sin(2*math.Pi*hourOfDay/12.0)
	baseValue += dailyCycle

	return baseValue
}

// generateReading generates a single sensor reading with operational patterns
func generateReading(readingID int64, timestamp time.Time, tagID, paramType string, seed int64) SensorReading {
	rng := rand.New(rand.NewSource(seed))

	baseValue := generateBaseValue(paramType, timestamp, seed)
	spec := parameterSpecs[paramType]

	// Determine operational pattern (75% normal, 15% drift, 8% excursions, 2% errors)
	patternRoll := rng.Float64()

	var measuredValue float64
	var qualityFlag string

	if patternRoll < 0.75 {
		// Normal operation: small random noise
		noise := (spec.MaxNormal - spec.MinNormal) * 0.02 * (rng.Float64() - 0.5)
		measuredValue = baseValue + noise
		qualityFlag = "Good"
	} else if patternRoll < 0.90 {
		// Minor drift: gradual trend toward limits
		driftFactor := 1.0 + 0.15*rng.Float64()
		measuredValue = baseValue * driftFactor
		if rng.Float64() < 0.3 {
			qualityFlag = "Questionable"
		} else {
			qualityFlag = "Good"
		}
	} else if patternRoll < 0.98 {
		// IOW excursion: brief period outside normal range
		excursionFactor := 1.0 + 0.3*rng.Float64()
		measuredValue = baseValue * excursionFactor
		qualityFlag = "Good"
	} else {
		// Sensor error or maintenance
		if rng.Float64() < 0.5 {
			// Bad reading
			measuredValue = spec.MinLimit + (spec.MaxLimit-spec.MinLimit)*rng.Float64()
			qualityFlag = "Bad"
		} else {
			// Substituted value during maintenance
			measuredValue = (spec.MinNormal + spec.MaxNormal) / 2.0
			qualityFlag = "Substituted"
		}
	}

	// Clamp to physical limits
	if measuredValue < spec.MinLimit {
		measuredValue = spec.MinLimit
	}
	if measuredValue > spec.MaxLimit {
		measuredValue = spec.MaxLimit
	}

	return SensorReading{
		ReadingID:       readingID,
		Timestamp:       timestamp,
		TagID:           tagID,
		ParameterType:   paramType,
		MeasuredValue:   measuredValue,
		DataQualityFlag: qualityFlag,
	}
}

// loadAssets loads asset information from dim_asset.csv
func loadAssets(seedsDir string) ([]Asset, error) {
	assetFile := filepath.Join(seedsDir, "dim_asset.csv")
	f, err := os.Open(assetFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open dim_asset.csv: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read dim_asset.csv: %w", err)
	}

	if len(records) < 2 {
		return nil, fmt.Errorf("dim_asset.csv is empty or missing header")
	}

	// Parse header to find column indices
	header := records[0]
	tagIDIdx := -1
	equipmentNameIdx := -1
	unitNameIdx := -1

	for i, col := range header {
		switch col {
		case "tag_id":
			tagIDIdx = i
		case "equipment_name":
			equipmentNameIdx = i
		case "unit_name":
			unitNameIdx = i
		}
	}

	if tagIDIdx == -1 || equipmentNameIdx == -1 || unitNameIdx == -1 {
		return nil, fmt.Errorf("dim_asset.csv missing required columns")
	}

	assets := []Asset{}
	for _, record := range records[1:] {
		assets = append(assets, Asset{
			TagID:         record[tagIDIdx],
			EquipmentName: record[equipmentNameIdx],
			UnitName:      record[unitNameIdx],
		})
	}

	return assets, nil
}

// generateSensorData generates sensor readings for all assets over 1 month
func generateSensorData(assets []Asset, startDate time.Time, numDays int) []SensorReading {
	readings := []SensorReading{}
	readingID := int64(1)

	// 5-minute intervals: 12 readings per hour
	intervalsPerDay := 24 * 12

	for _, asset := range assets {
		sensorTypes := determineSensorTypes(asset.EquipmentName)

		for _, sensorType := range sensorTypes {
			// Generate readings for this sensor over the time period
			for day := 0; day < numDays; day++ {
				for interval := 0; interval < intervalsPerDay; interval++ {
					timestamp := startDate.Add(time.Duration(day*24*60+interval*5) * time.Minute)

					// Use tag ID and timestamp as seed for reproducibility
					seed := int64(hashString(asset.TagID)) + timestamp.Unix()

					reading := generateReading(readingID, timestamp, asset.TagID, sensorType, seed)
					readings = append(readings, reading)
					readingID++
				}
			}
		}
	}

	return readings
}

// hashString creates a simple hash of a string for seeding
func hashString(s string) int {
	hash := 0
	for _, c := range s {
		hash = hash*31 + int(c)
	}
	return hash
}

// writeSensorReadingsCSV writes sensor readings to CSV file
func writeSensorReadingsCSV(readings []SensorReading, outputPath string) error {
	f, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	// Write header
	header := []string{
		"reading_id",
		"timestamp",
		"tag_id",
		"parameter_type",
		"measured_value",
		"data_quality_flag",
	}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write readings
	for _, reading := range readings {
		record := []string{
			strconv.FormatInt(reading.ReadingID, 10),
			reading.Timestamp.Format("2006-01-02 15:04:05"),
			reading.TagID,
			reading.ParameterType,
			strconv.FormatFloat(reading.MeasuredValue, 'f', 2, 64),
			reading.DataQualityFlag,
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	return nil
}

func main() {
	// Determine seeds directory (relative to this script)
	seedsDir := filepath.Join("..", "seeds")

	// Load assets from dim_asset.csv
	fmt.Println("Loading assets from dim_asset.csv...")
	assets, err := loadAssets(seedsDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading assets: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Loaded %d assets\n", len(assets))

	// Generate sensor data for January 2025 (30 days)
	startDate := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	numDays := 30

	fmt.Printf("Generating sensor data for %d days starting %s...\n", numDays, startDate.Format("2006-01-02"))
	readings := generateSensorData(assets, startDate, numDays)
	fmt.Printf("Generated %d sensor readings\n", len(readings))

	// Write to CSV
	outputPath := filepath.Join(seedsDir, "raw_sensor_readings.csv")
	fmt.Printf("Writing sensor readings to %s...\n", outputPath)
	if err := writeSensorReadingsCSV(readings, outputPath); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing sensor readings: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Sensor data generation complete!")
}
