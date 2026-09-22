[CmdletBinding()]
param(
    [switch]$Check,
    [switch]$EnableDeveloperModeOnly,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$BootstrapArguments = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repositoryRoot = Split-Path -Parent $PSCommandPath
$developerModeKey = "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\AppModelUnlock"
$developerModeName = "AllowDevelopmentWithoutDevLicense"

if ($BootstrapArguments.Count -gt 0) {
    if ($BootstrapArguments.Count -eq 1 -and $BootstrapArguments[0] -eq "--check") {
        $Check = $true
    }
    else {
        throw "Usage: .\bootstrap.ps1 [--check]"
    }
}

function Test-Administrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Test-DeveloperMode {
    $setting = Get-ItemProperty -LiteralPath $developerModeKey -Name $developerModeName -ErrorAction SilentlyContinue
    return $null -ne $setting -and $setting.$developerModeName -eq 1
}

function Enable-DeveloperMode {
    if (-not (Test-Administrator)) {
        throw "Administrator approval is required to enable Windows Developer Mode."
    }
    if (-not (Test-Path -LiteralPath $developerModeKey)) {
        New-Item -Path $developerModeKey -Force | Out-Null
    }
    New-ItemProperty `
        -LiteralPath $developerModeKey `
        -Name $developerModeName `
        -PropertyType DWord `
        -Value 1 `
        -Force | Out-Null
}

function Resolve-UvPath {
    $command = Get-Command uv -ErrorAction SilentlyContinue
    if ($null -ne $command) {
        return $command.Source
    }
    $wingetLink = Join-Path $env:LOCALAPPDATA "Microsoft\WinGet\Links\uv.exe"
    if (Test-Path -LiteralPath $wingetLink) {
        return $wingetLink
    }
    return $null
}

function Resolve-GitPath {
    $command = Get-Command git -ErrorAction SilentlyContinue
    if ($null -ne $command) {
        return $command.Source
    }
    $installedGit = Join-Path $env:ProgramFiles "Git\cmd\git.exe"
    if (Test-Path -LiteralPath $installedGit) {
        return $installedGit
    }
    return $null
}

function Test-UvVersion {
    param([Parameter(Mandatory = $true)][string]$UvPath)
    $versionOutput = & $UvPath --version
    if ($LASTEXITCODE -ne 0 -or $versionOutput -notmatch '^uv (\d+\.\d+\.\d+)') {
        return $false
    }
    return [version]$Matches[1] -ge [version]"0.12.0"
}

function Test-HatchVersion {
    param([Parameter(Mandatory = $true)][string]$UvPath)
    $toolOutput = & $UvPath tool list
    return $LASTEXITCODE -eq 0 -and $toolOutput -match '(?m)^hatch v1\.18\.1$'
}

function Invoke-NativeCommand {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code $LASTEXITCODE`: $FilePath"
    }
}

if ($EnableDeveloperModeOnly) {
    Enable-DeveloperMode
    exit 0
}

if ($Check) {
    $gitPath = Resolve-GitPath
    if ($null -eq $gitPath) {
        throw "Git is missing. Run .\bootstrap.ps1 without --check first."
    }
    $uvPath = Resolve-UvPath
    if ($null -eq $uvPath) {
        throw "uv is missing. Run .\bootstrap.ps1 without --check first."
    }
    $env:PATH = (Split-Path -Parent $gitPath) + [IO.Path]::PathSeparator + `
        (Split-Path -Parent $uvPath) + [IO.Path]::PathSeparator + $env:PATH
    if (-not (Test-UvVersion $uvPath)) {
        throw "uv 0.12 or newer is required. Run .\bootstrap.ps1 without --check first."
    }
    if (-not (Test-HatchVersion $uvPath)) {
        throw "Hatch 1.18.1 is missing. Run .\bootstrap.ps1 without --check first."
    }
    if (-not (Test-DeveloperMode)) {
        throw "Windows Developer Mode is disabled. Run .\bootstrap.ps1 without --check first."
    }
    $pythonOutput = & $uvPath --no-python-downloads python find --system --managed-python "3.12"
    if ($LASTEXITCODE -ne 0) {
        throw "uv-managed Python 3.12 is missing. Run .\bootstrap.ps1 without --check first."
    }
    $pythonPath = ($pythonOutput | Select-Object -Last 1).Trim()
    & $pythonPath (Join-Path $repositoryRoot "scripts\bootstrap.py") "--check"
    exit $LASTEXITCODE
}

$wingetCommand = Get-Command winget -ErrorAction SilentlyContinue
if ($null -eq $wingetCommand) {
    throw "winget is required. Install Microsoft App Installer, then rerun this script."
}
$wingetPath = $wingetCommand.Source

if ($null -eq (Resolve-GitPath)) {
    Invoke-NativeCommand $wingetPath @(
        "install", "--id", "Git.Git", "--exact", "--source", "winget",
        "--accept-package-agreements", "--accept-source-agreements"
    )
}
$existingUvPath = Resolve-UvPath
if ($null -eq $existingUvPath) {
    Invoke-NativeCommand $wingetPath @(
        "install", "--id", "astral-sh.uv", "--exact", "--source", "winget",
        "--accept-package-agreements", "--accept-source-agreements"
    )
}
elseif (-not (Test-UvVersion $existingUvPath)) {
    Invoke-NativeCommand $wingetPath @(
        "upgrade", "--id", "astral-sh.uv", "--exact", "--source", "winget",
        "--accept-package-agreements", "--accept-source-agreements"
    )
}

$gitPath = Resolve-GitPath
if ($null -eq $gitPath) {
    throw "Git was installed but could not be resolved. Start a new terminal and rerun this script."
}
$uvPath = Resolve-UvPath
if ($null -eq $uvPath) {
    throw "uv was installed but could not be resolved. Start a new terminal and rerun this script."
}
$env:PATH = (Split-Path -Parent $gitPath) + [IO.Path]::PathSeparator + `
    (Split-Path -Parent $uvPath) + [IO.Path]::PathSeparator + $env:PATH
if (-not (Test-UvVersion $uvPath)) {
    throw "uv 0.12 or newer is required after installation."
}

if (-not (Test-DeveloperMode)) {
    if (Test-Administrator) {
        Enable-DeveloperMode
    }
    else {
        $currentPowerShell = (Get-Process -Id $PID).Path
        $quotedScriptPath = '"' + $PSCommandPath + '"'
        $elevated = Start-Process `
            -FilePath $currentPowerShell `
            -ArgumentList @(
                "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                $quotedScriptPath, "-EnableDeveloperModeOnly"
            ) `
            -Verb RunAs `
            -Wait `
            -PassThru `
            -WindowStyle Hidden
        if ($elevated.ExitCode -ne 0 -or -not (Test-DeveloperMode)) {
            throw "Windows Developer Mode was not enabled. Approve the administrator prompt and rerun."
        }
    }
}

Invoke-NativeCommand $uvPath @("python", "install", "--upgrade", "3.12")
Invoke-NativeCommand $uvPath @("tool", "install", "--force", "hatch==1.18.1")
Invoke-NativeCommand $uvPath @(
    "run", "--no-project", "--python", "3.12",
    (Join-Path $repositoryRoot "scripts\bootstrap.py")
)
