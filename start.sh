#!/bin/bash

# Define the range of values for NUM_LOGS
start=2000000
end=10000000
increment=2000000

# baseline
    echo "Running with NUM_LOGS=1000000"
    ./main 10000000
    echo "------------------------------------"

# Iterate over the range and run the program
for ((num=start; num<=end; num+=increment))
do
    echo "Running with NUM_LOGS=$num"
    ./main $num
    echo "------------------------------------"
done

