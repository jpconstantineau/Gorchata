# Fix remaining compilation issues in integration tests

# Issue 1: Fix dbPath capture in test_execution_test.go
$file = "test\integration\test_execution_test.go"
$lines = Get-Content $file
for ($i = 0; $i < $lines.Count; $i++) {
    # Line 24: TestIntegration_ExecuteTestsEndToEnd
    if ($i -eq 23 -and $lines[$i] -match 'adapter, _ := CreateTestDatabase') {
        $lines[$i] = "`tadapter, dbPath := CreateTestDatabase(t)"
    }
    # Lines with selector.Select -> selector.Filter
    if ($lines[$i] -match 'selector(\d)\.Select\(') {
        $lines[$i] = $lines[$i] -replace '\.Select\(', '.Filter('
    }
}
$lines | Set-Content $file

# Issue 2: Fix test_storage_test.go ExecuteDDL with timestamps
$file = "test\integration\test_storage_test.go"  
$content = Get-Content $file -Raw

# Find the INSERT statements with timestamps and use proper SQL parameter placeholders
$content = $content -replace 'INSERT INTO (\w+) \(test_id, test_run_id, failed_at, id\) VALUES\s+\(''test1'', ''run1'', \?, 1\),\s+\(''test1'', ''run2'', \?, 2\),\s+\(''test1'', ''run3'', \?, 3\)', 
    'INSERT INTO $1 (test_id, test_run_id, failed_at, id) SELECT ''test1'', ''run1'', ?, 1 UNION SELECT ''test1'', ''run2'', ?, 2 UNION SELECT ''test1'', ''run3'', ?, 3'

Set-Content $file -Value $content -NoNewline

Write-Host "Fix script complete. Please review changes."
