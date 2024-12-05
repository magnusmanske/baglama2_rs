#!/bin/bash
toolforge jobs delete single 2> /dev/null

rm ~/single.*

toolforge jobs run --mem 3000Mi --cpu 3 --mount=all --filelog --image tool-glamtools/tool-glamtools:latest \
--command "sh -c 'RUST_BACKTRACE=1 target/release/baglama2 $1 $2 $3 $4'" single
