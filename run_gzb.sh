#!/bin/bash
# Launch gzb jobs on Toolforge (run as tools.glamtools from ~/baglama2_rs).
#
#   ./run_gzb.sh check 2026 9            preflight only: dump, replicas, DB, dirs
#   ./run_gzb.sh month 2026 9 [FLAGS]    check, then generate a month (resumable: just re-run)
#   ./run_gzb.sh convert [FLAGS]         convert legacy data, e.g. --storage=file
#   ./run_gzb.sh tsv 979 2026 9 [WIKI]   export to ~/gzb-tsv/979-2026-9[-WIKI].tsv
#   ./run_gzb.sh schedule                monthly cron: last month, on the 3rd
#
# FLAGS are passed through; see `target/release/baglama2 help`.
# Logs go to ~/gzb-<job>.out and ~/gzb-<job>.err.
set -euo pipefail

IMAGE=tool-glamtools/tool-glamtools:latest
BIN=target/release/baglama2

run_job() { # name mem command...
	local name=$1 mem=$2
	shift 2
	toolforge jobs delete "$name" 2>/dev/null || true
	rm -f "$HOME/$name.out" "$HOME/$name.err"
	toolforge jobs run --mem "$mem" --cpu 3 --mount=all --image "$IMAGE" \
		--command "$BIN $*" \
		--filelog -o "$HOME/$name.out" -e "$HOME/$name.err" "$name"
	echo "Started $name; follow with: tail -f ~/$name.err"
}

# Month jobs share the replicas' per-tool connection limit (10 per cluster),
# so two at once starve each other. Prints running month jobs other than $1.
other_month_jobs() {
	toolforge jobs list | awk -F'|' -v self="$1" '
		{ name = $2; gsub(/^ +| +$/, "", name) }
		name != self && name ~ /^gzb-(monthly|[0-9lm]+-[0-9lm]+)$/ && $4 ~ /Running/ { print name }'
}

cmd=${1:-}
case "$cmd" in
check | month)
	year=${2:?year (or lm) expected}
	month=${3:?month (or lm) expected}
	shift 3
	if [ "$cmd" = check ]; then
		run_job "gzb-check-$year-$month" 1Gi gzb_check "$year" "$month" "$@"
	else
		running=$(other_month_jobs "gzb-$year-$month")
		if [ -n "$running" ]; then
			echo "Not starting: month job(s) still running: $running" >&2
			echo "Run months one at a time (replica connection limit; see GZB.md)." >&2
			exit 1
		fi
		run_job "gzb-$year-$month" 3Gi gzb_month "$year" "$month" "$@"
	fi
	;;
convert)
	shift
	run_job gzb-convert 5Gi gzb_convert "$@"
	;;
tsv)
	group=${2:?group ID expected}
	year=${3:?year expected}
	month=${4:?month expected}
	wiki=${5:-}
	mkdir -p "$HOME/gzb-tsv"
	out="$HOME/gzb-tsv/$group-$year-$month${wiki:+-$wiki}.tsv"
	run_job "gzb-tsv-$group-$year-$month" 1Gi gzb_tsv "$group" "$year" "$month" $wiki "--out=$out"
	;;
schedule)
	toolforge jobs delete gzb-monthly 2>/dev/null || true
	toolforge jobs run --mem 3Gi --cpu 3 --mount=all --image "$IMAGE" \
		--command "$BIN gzb_month lm lm" \
		--schedule "17 3 3 * *" \
		--filelog -o "$HOME/gzb-monthly.out" -e "$HOME/gzb-monthly.err" gzb-monthly
	;;
*)
	sed -n '2,11p' "$0"
	exit 1
	;;
esac
