#!/bin/bash
toolforge jobs delete rustbot
\rm ~/rustbot.*
toolforge jobs run --mem 4000Mi --cpu 3 --continuous --mount=all \
	--image glamtools/glamtools:latest \
	--command "target/release/baglama2" \
	--filelog -o /data/project/glamtools/rustbot.out -e /data/project/glamtools/rustbot.err \
	rustbot
