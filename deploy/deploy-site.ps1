# Deploy the static project page (site/) to raftkv.dash-board.in.
# Run on the DEV machine (PowerShell). Pattern shared with the other
# dash-board.in projects: zip -> scp -> Expand-Archive -> Caddy site block.
#
#   ./deploy/deploy-site.ps1
#
# The VPS serves it from C:\dashboard\sites\raftkv behind Caddy (auto-HTTPS,
# wildcard DNS already points *.dash-board.in at the box). Redeploy = re-run.
param(
  [string]$Key = "$HOME\.ssh\dashboard_vps",
  [string]$VpsHost = 'Administrator@144.172.98.43'
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent

$zip = Join-Path $env:TEMP 'raftkv-site.zip'
Compress-Archive -Path (Join-Path $root 'site\*') -DestinationPath $zip -Force
scp -i $Key $zip "${VpsHost}:C:/dashboard/raftkv-site.zip"
Remove-Item $zip -Force

# Ship the Caddy site block as a file — inline multi-line strings do not survive
# ssh's re-quoting into the remote PowerShell.
$block = Join-Path $env:TEMP 'raftkv.caddyblock'
@"

raftkv.dash-board.in {
	root * C:/dashboard/sites/raftkv
	encode gzip zstd
	file_server
}
"@ | Out-File -FilePath $block -Encoding ascii
scp -i $Key $block "${VpsHost}:C:/dashboard/raftkv.caddyblock"
Remove-Item $block -Force

$remote = @'
New-Item -ItemType Directory -Force -Path C:\dashboard\sites\raftkv | Out-Null
Remove-Item C:\dashboard\sites\raftkv\* -Recurse -Force -ErrorAction SilentlyContinue
Expand-Archive -Path C:\dashboard\raftkv-site.zip -DestinationPath C:\dashboard\sites\raftkv -Force
Remove-Item C:\dashboard\raftkv-site.zip -Force
$caddyfile = 'C:\dashboard\Caddyfile'
if (-not (Select-String -Path $caddyfile -Pattern 'raftkv\.dash-board\.in' -Quiet)) {
  Get-Content C:\dashboard\raftkv.caddyblock | Add-Content -Path $caddyfile
  C:\caddy.exe reload --config $caddyfile
  Write-Output 'caddy: site block added + reloaded'
} else {
  Write-Output 'caddy: site block already present'
}
Remove-Item C:\dashboard\raftkv.caddyblock -Force -ErrorAction SilentlyContinue
'@
ssh -i $Key $VpsHost $remote
Write-Host 'Deployed: https://raftkv.dash-board.in'
