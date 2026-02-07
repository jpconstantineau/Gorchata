package materialization

import "fmt"

// GetStrategy returns the appropriate materialization strategy for the given type
func GetStrategy(matType MaterializationType) (Strategy, error) {
	switch matType {
	case MaterializationView:
		return &ViewStrategy{}, nil
	case MaterializationTable:
		return &TableStrategy{}, nil
	case MaterializationIncremental:
		return &IncrementalStrategy{}, nil
	default:
		return nil, fmt.Errorf("unknown materialization type: %s", matType)
	}
}

// GetStrategyFromConfig returns the appropriate strategy based on the config
// If no type is specified, defaults to table materialization
func GetStrategyFromConfig(config MaterializationConfig) (Strategy, error) {
	// Default to table if no type specified
	matType := config.Type
	if matType == "" {
		matType = MaterializationTable
	}

	return GetStrategy(matType)
}
