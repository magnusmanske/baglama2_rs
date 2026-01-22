#!/bin/bash
# parameters: year month
toolforge jobs delete views 2> /dev/null

rm ~/views.*

toolforge jobs run --mem 500Mi --retry 5 --mount=all --filelog --image tool-glamtools/tool-glamtools:latest \
--command "sh -c 'RUST_BACKTRACE=1 target/release/baglama2 mysql2_views $1 $2'" views
