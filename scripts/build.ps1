# Builds the React console, runs Java + console tests, and packages target/conductor.jar.
$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
& ./mvnw.cmd -B package @args
if ($LASTEXITCODE -ne 0) { throw 'Build failed' }
