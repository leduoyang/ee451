#!/bin/bash

# Define the range of values for NUM_LOGS
start=1000000
end=10000000
increment=1000000

# Iterate over the range and run the program
for ((num=start; num<=end; num+=increment))
do
    echo "Running with NUM_LOGS=$num"
    ./main $num
    echo "------------------------------------"
done

