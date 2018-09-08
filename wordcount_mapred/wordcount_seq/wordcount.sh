#!/usr/bin/env bash
# remove the output results if already existed
if [ -f wordcount_output ]; then
    echo "[INFO] Deleting wordcount_output if exists..."
    rm wordcount_output
fi
# remove the map intermediate output if already existed
for file in map*; do
    if [ -f "$file" ]; then
        echo "[INFO] Deleting the intermediate output of the map step if exists..."
        rm "$file"
    fi
done
# map phase
for f in ../input/*; do
    <"$f" java -cp ../target/project1.jar edu.cmu.scs.cc.project1.WordCountSequentialMap >> mapout.$$
done
# reduce phase
LC_ALL=C sort -k1,1i mapout.$$ | java -cp ../target/project1.jar edu.cmu.scs.cc.project1.WordCountSequentialReduce >> output
LC_ALL=C sort -k2,2nr -k1,1 output > wordcount_output
rm output