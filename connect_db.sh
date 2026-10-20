#!/bin/bash
# SSH tunnels to the Toolforge databases for local runs: one per entry in the
# [tunnels] section of replica.my.cnf next to this script (gitignored; a copy
# of the tool's, plus local='true'), all through one ssh process that is
# restarted if it exits. Runs in the foreground.
#
# Needs toolforge-tunnel: cargo install toolforge --features=cli
TOOL_DATA_DIR="$(cd "$(dirname "$0")" && pwd)" exec toolforge-tunnel "$@"
