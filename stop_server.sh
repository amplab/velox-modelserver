#!/usr/bin/env sh

pid=$(ps aux | grep [v]elox | tr -s " " | cut -d " " -f 2)
kill -15 $pid
