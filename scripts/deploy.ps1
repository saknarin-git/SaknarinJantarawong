param(
    [string]$DeploymentId = "AKfycbyfYwUMqroaI6ND4AdW5hfhUp_FIV1PVgEp9ggtPmWiqooB5r4tkrMvv2sIzPHQfx3H-Q",
    [string]$Description = "auto deploy",
    [string]$StageDir = "$env:USERPROFILE\gas-deploy-temp"
)

$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot
Set-Location ..
$projectRoot = $PWD.Path

New-Item -ItemType Directory -Path $StageDir -Force | Out-Null

$requiredFiles = @("Code.gs", "Index.html", "appsscript.json", ".clasp.json")
foreach ($file in $requiredFiles) {
    $source = Join-Path $projectRoot $file
    if (-not (Test-Path $source)) {
        throw "Missing required file: $source"
    }

    Copy-Item -Path $source -Destination (Join-Path $StageDir $file) -Force
}

Set-Location $StageDir

$fallback = "C:\Users\Bill\clasp-global\clasp.cmd"
$localClasp = Join-Path $projectRoot "node_modules/.bin/clasp.cmd"

if (Test-Path $fallback) {
    $claspCmd = $fallback
} elseif (Test-Path $localClasp) {
    $claspCmd = $localClasp
} else {
    throw "Cannot find clasp command. Install @google/clasp locally or set up C:\Users\Bill\clasp-global\clasp.cmd"
}

function Invoke-Clasp {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Args
    )

    $quotedArgs = @()
    foreach ($arg in $Args) {
        $safeArg = $arg -replace '"', '\\"'
        $quotedArgs += ('"{0}"' -f $safeArg)
    }

    $cmdLine = ('"{0}" {1}' -f $claspCmd, ($quotedArgs -join ' '))
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $Global:ErrorActionPreference = "Continue"
        $output = cmd /d /s /c $cmdLine 2>&1 | ForEach-Object { "$_" }
        $exitCode = $LASTEXITCODE
    }
    finally {
        $Global:ErrorActionPreference = $previousErrorActionPreference
    }

    if ($exitCode -ne 0) {
        throw "clasp command failed: $($Args -join ' ')`n$($output | Out-String)"
    }

    return $output
}

Write-Host "[0/4] Preparing clasp session..."
Write-Host "[0/4] Using stage directory: $StageDir"

$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm"
$versionDescription = "$Description $timestamp"

Write-Host "[1/4] Pushing files..."
try {
    Invoke-Clasp push -f | Out-Null
}
catch {
    $pushMessage = $_.Exception.Message
    if ($pushMessage -match "(?i)login|auth|credential|unauthorized") {
        Write-Host "[1/4] Clasp session not ready. Opening login flow..."
        Invoke-Clasp login | Out-Null
        Invoke-Clasp push -f | Out-Null
    }
    else {
        throw
    }
}

Write-Host "[2/4] Creating new version..."
$versionOutput = Invoke-Clasp version $versionDescription

$versionNumber = $null
foreach ($line in $versionOutput) {
    if ($line -match "Created version (\d+)\.") {
        $versionNumber = $matches[1]
    }
}

if (-not $versionNumber) {
    throw "Unable to detect created version number from clasp output."
}

Write-Host "[3/4] Updating deployment $DeploymentId to version $versionNumber..."
Invoke-Clasp deploy --deploymentId $DeploymentId --versionNumber $versionNumber --description $versionDescription | Out-Null

Write-Host "[4/4] Done. Deployment updated to version $versionNumber"