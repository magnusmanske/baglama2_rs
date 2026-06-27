#!/bin/bash
toolforge jobs delete single 2> /dev/null

rm ~/single.*

toolforge jobs run --mem 2Gi --cpu 2 --retry 5 --mount=all --filelog --image tool-glamtools/tool-glamtools:latest \
--command "target/release/baglama2 $1 $2 $3 $4" single
