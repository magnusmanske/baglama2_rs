#!/bin/bash
toolforge jobs delete single

toolforge jobs run --wait --mem 2000Mi --cpu 1 --mount=all --image tool-glamtools/tool-glamtools:latest \
--command "sh -c 'target/release/baglama2 $1 $2 $3 $4'" single

toolforge jobs logs single
