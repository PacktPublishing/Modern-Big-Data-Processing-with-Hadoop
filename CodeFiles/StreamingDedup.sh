#!/bin/bash

for i in $(seq 1 10)
do
	for j in $(seq 1 5)
	do
		sleep 1
		tail -n+$(($i * 3)) /usr/share/dict/words | head -3
	done
done | nc -l 5000
