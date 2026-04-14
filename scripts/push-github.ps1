param(
    [string]$RemoteName = "origin",
    [string]$Branch = "main",
    [string]$Message = "auto backup",
    [string]$RemoteUrl = ""
)

$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot
Set-Location ..
$projectRoot = $PWD.Path

$gitCandidates = @(
    "git",
    "$env:LOCALAPPDATA\Programs\Git\cmd\git.exe",
    "C:\Program Files\Git\cmd\git.exe"
)

$git = $null
foreach ($candidate in $gitCandidates) {
    if ($candidate -eq "git") {
        $null = Get-Command git -ErrorAction SilentlyContinue
        if ($?) {
            $git = "git"
            break
        }
    } elseif (Test-Path $candidate) {
        $git = $candidate
        break
    }
}

if (-not $git) {
    throw "Git is not available. Install Git first."
}

function Invoke-Git {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Command
    )

    & $git @Command
    if ($LASTEXITCODE -ne 0) {
        throw "git command failed: $($Command -join ' ')"
    }
}

Set-Location $projectRoot

$insideRepo = $true
try {
    & $git rev-parse --is-inside-work-tree | Out-Null
    if ($LASTEXITCODE -ne 0) {
        $insideRepo = $false
    }
} catch {
    $insideRepo = $false
}

if (-not $insideRepo) {
    Write-Host "[1/6] Initializing git repository..."
    Invoke-Git @("init")
}

Write-Host "[2/6] Ensuring branch $Branch..."
$currentBranch = (& $git branch --show-current 2>$null)
if ([string]::IsNullOrWhiteSpace($currentBranch)) {
    Invoke-Git @("checkout", "-b", $Branch)
} elseif ($currentBranch.Trim() -ne $Branch) {
    $branchExists = (& $git branch --list $Branch)
    if ([string]::IsNullOrWhiteSpace($branchExists)) {
        Invoke-Git @("checkout", "-b", $Branch)
    } else {
        Invoke-Git @("checkout", $Branch)
    }
}

if (-not [string]::IsNullOrWhiteSpace($RemoteUrl)) {
    Write-Host "[3/6] Configuring remote $RemoteName..."
    $existingRemote = (& $git remote)
    if (($existingRemote -split "`n") -contains $RemoteName) {
        Invoke-Git @("remote", "set-url", $RemoteName, $RemoteUrl)
    } else {
        Invoke-Git @("remote", "add", $RemoteName, $RemoteUrl)
    }
}

$remoteUrl = (& $git remote get-url $RemoteName 2>$null)
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($remoteUrl)) {
    throw "Remote '$RemoteName' is not configured. Run: git remote add $RemoteName https://github.com/<user>/<repo>.git"
}

Write-Host "[4/6] Staging changes..."
Invoke-Git @("add", "-A")

$status = (& $git status --porcelain)
if (-not [string]::IsNullOrWhiteSpace(($status | Out-String).Trim())) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm"
    $commitMessage = "$Message $timestamp"
    Write-Host "[5/6] Committing: $commitMessage"
    Invoke-Git @("commit", "-m", $commitMessage)
} else {
    Write-Host "[5/6] No file changes to commit."
}

Write-Host "[6/6] Pushing to $RemoteName/$Branch..."
Invoke-Git @("push", "-u", $RemoteName, $Branch)

Write-Host "Done. Pushed to GitHub successfully."