# Remove defer adapter.Close() lines after CreateTestDatabase calls
$files = @(
    "test\integration\test_storage_test.go",
    "test\integration\test_execution_test.go",
    "test\integration\cli_integration_test.go",
    "test\integration\adaptive_sampling_test.go"
)

foreach ($file in $files) {
    $fullPath = Join-Path $PSScriptRoot "..\$file"
    if (Test-Path $fullPath) {
        Write-Host "Processing: $file"
        $lines = Get-Content $fullPath
        $newLines = @()
        $skipNext = $false
        
        for ($i = 0; $i -lt $lines.Count; $i++) {
            if ($lines[$i] -match '^\s+defer adapter\.Close\(\)\s*$') {
                # Check if previous line has CreateTestDatabase
                if ($i -gt 0 -and $newLines[-1] -match 'CreateTestDatabase\(t\)') {
                    Write-Host "  Removing line $($i+1): $($lines[$i])"
                    continue
                }
            }
            $newLines += $lines[$i]
        }
        
        $newLines | Set-Content $fullPath -Force
        Write-Host "  Done"
    }
}

Write-Host "`nAll files processed"
