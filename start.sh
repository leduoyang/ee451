#!/bin/bash

# first exp
echo "================= serial vs parallel experiment ======================="
parameters=(1 2 4 8 16 32 64)
for param in "${parameters[@]}"; do
    echo "Running ./main with parameter: $param"
    ./main $param $param $param
    echo "========================================"
done
sleep 10

# second exp
echo "================= producer experiment ======================="
parameters=(1 2 4 8 16 32 64)
for param in "${parameters[@]}"; do
    echo "Running ./main with parameter: $param"
    ./main $param 16 16
    echo "========================================"
    sleep 10
done

# third exp
echo "================= consumer experiment ======================="
parameters=(1 2 4 8 16 32 64)
for param in "${parameters[@]}"; do
    echo "Running ./main with parameter: $param"
    ./main 16 $param $param
    echo "========================================"
    sleep 10
done

# forth exp
echo "================= partition experiment ======================="
parameters=(1 2 4 8 16 32 64)
for param in "${parameters[@]}"; do
    echo "Running ./main with parameter: $param"
    ./main 16 16 $param
    echo "========================================"
    sleep 10
done

# fifth exp
echo "================= batch experiment ======================="
parameters=(1 10 100 1000 10000 100000 1000000)
for param in "${parameters[@]}"; do
    echo "Running ./main with parameter: $param"
    ./main 16 16 16 $param
    echo "========================================"
    sleep 10
done