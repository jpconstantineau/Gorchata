#!/usr/bin/env pwsh
# Fix {{ ref }} syntax in test files - replace with direct table names
# This script updates all test SQL files to use direct table references instead of {{ ref }} syntax

$ErrorActionPreference = "Stop"

Write-Host "Fixing {{ ref }} syntax in test files..." -ForegroundColor Cyan

$testDir = "$PSScriptRoot\..\examples\precision_railroading\tests"
$testFiles = Get-ChildItem -Path $testDir -Filter "*.sql" -Recurse

$replacements = @(
    @{ pattern = '\{\{ ref "stg_clm_events" \}\}'; replacement = 'stg_clm_events' },
    @{ pattern = '\{\{ ref "stg_clm_enriched" \}\}'; replacement = 'stg_clm_enriched' },
    @{ pattern = '\{\{ ref "int_state_intervals" \}\}'; replacement = 'int_state_intervals' },
    @{ pattern = '\{\{ ref "int_trip_segments" \}\}'; replacement = 'int_trip_segments' },
    @{ pattern = '\{\{ ref "int_velocity_vectors" \}\}'; replacement = 'int_velocity_vectors' },
    @{ pattern = '\{\{ ref "int_nodal_dwell" \}\}'; replacement = 'int_nodal_dwell' },
    @{ pattern = '\{\{ ref "int_dwell_classification" \}\}'; replacement = 'int_dwell_classification' },
    @{ pattern = '\{\{ ref "int_cycle_classification" \}\}'; replacement = 'int_cycle_classification' },
    @{ pattern = '\{\{ ref "fact_trip" \}\}'; replacement = 'fact_trip' },
    @{ pattern = '\{\{ ref "fact_dwell" \}\}'; replacement = 'fact_dwell' },
    @{ pattern = '\{\{ ref "fact_stop_classification" \}\}'; replacement = 'fact_stop_classification' },
    @{ pattern = '\{\{ ref "fact_corridor_transit" \}\}'; replacement = 'fact_corridor_transit' },
    @{ pattern = '\{\{ ref "agg_network_fluidity" \}\}'; replacement = 'agg_network_fluidity' },
    @{ pattern = '\{\{ ref "agg_slot_adherence" \}\}'; replacement = 'agg_slot_adherence' },
    @{ pattern = '\{\{ ref "agg_shadow_yards" \}\}'; replacement = 'agg_shadow_yards' },
    @{ pattern = '\{\{ ref "agg_buffer_consumption" \}\}'; replacement = 'agg_buffer_consumption' },
    @{ pattern = '\{\{ ref "agg_directional_asymmetry" \}\}'; replacement = 'agg_directional_asymmetry' },
    @{ pattern = '\{\{ ref "agg_corridor_weekly_performance" \}\}'; replacement = 'agg_corridor_weekly_performance' },
    @{ pattern = '\{\{ ref "agg_psr_evolution" \}\}'; replacement = 'agg_psr_evolution' },
    @{ pattern = '\{\{ ref "shadow_yard_identification" \}\}'; replacement = 'shadow_yard_identification' },
    @{ pattern = '\{\{ ref "worst_performing_corridors" \}\}'; replacement = 'worst_performing_corridors' },
    @{ pattern = '\{\{ ref "seasonal_performance_trends" \}\}'; replacement = 'seasonal_performance_trends' },
    @{ pattern = '\{\{ ref "psr_strategy_shifts" \}\}'; replacement = 'psr_strategy_shifts' },
    @{ pattern = '\{\{ ref "network_congestion_hotspots" \}\}'; replacement = 'network_congestion_hotspots' },
    @{ pattern = '\{\{ ref "directional_efficiency_analysis" \}\}'; replacement = 'directional_efficiency_analysis' }
)

$totalFiles = 0
$totalReplacements = 0

foreach ($file in $testFiles) {
    $content = Get-Content -Path $file.FullName -Raw
    $originalContent = $content
    $fileReplacements = 0
    
    foreach ($r in $replacements) {
        $matches = [regex]::Matches($content, $r.pattern)
        if ($matches.Count -gt 0) {
            $content = $content -replace $r.pattern, $r.replacement
            $fileReplacements += $matches.Count
        }
    }
    
    if ($content -ne $originalContent) {
        Set-Content -Path $file.FullName -Value $content -NoNewline
        $totalFiles++
        $totalReplacements += $fileReplacements
        Write-Host "  ✓ Fixed $fileReplacements occurrences in $($file.Name)" -ForegroundColor Green
    }
}

Write-Host "`n✓ Complete: Fixed $totalReplacements {{ ref }} occurrences across $totalFiles files" -ForegroundColor Green
