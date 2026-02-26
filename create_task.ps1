$taskName    = "GmailScraper-Nightly"
$batFile     = "C:\Users\admin\Desktop\Projects\ai-projects\gmail-scraper\scheduled_gmail_scraper.bat"
$triggerTime = "23:00"

$action  = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$batFile`""
$trigger = New-ScheduledTaskTrigger -Daily -At $triggerTime
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
    -StartWhenAvailable

Register-ScheduledTask `
    -TaskName   $taskName `
    -Action     $action `
    -Trigger    $trigger `
    -Settings   $settings `
    -RunLevel   Limited `
    -Force

Write-Host "Task '$taskName' registered - runs daily at $triggerTime"
