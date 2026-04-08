param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs
)

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonScript = Join-Path $ScriptDir "start_all.py"

$pythonExe = $null
$pythonArgs = @()

if ($env:GISDATAMONITOR_PYTHON) {
    $pythonExe = $env:GISDATAMONITOR_PYTHON
}
elseif ($cmd = Get-Command python -ErrorAction SilentlyContinue) {
    $pythonExe = $cmd.Source
}
elseif ($cmd = Get-Command py -ErrorAction SilentlyContinue) {
    $pythonExe = $cmd.Source
    $pythonArgs += "-3"
}

if (-not $pythonExe) {
    Write-Error "Python interpreter not found. Install Python or set GISDATAMONITOR_PYTHON."
    exit 1
}

& $pythonExe @pythonArgs $PythonScript @ExtraArgs
exit $LASTEXITCODE
