#!/bin/bash
toolforge jobs delete single 2> /dev/null

toolforge jobs run --wait --mem 3000Mi --cpu 3 --mount=all --filelog --image tool-glamtools/tool-glamtools:latest \
--command "sh -c 'target/release/baglama2 $1 $2 $3 $4'" single

toolforge jobs logs single
