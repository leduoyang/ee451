#!/bin/bash

# first exp
echo "================= serial vs parallel experiment ======================="
parameters=(1 2 4 6 8 10 12 14 16 18)
for param in "${parameters[@]}"; do
    echo "Running ./main with parameter: $param"
    ./main $param $param $param
    echo "========================================"
done
sleep 10

# second exp
echo "================= producer experiment ======================="
parameters=(1 2 4 6 8 10 12 14 16 18)
for param in "${parameters[@]}"; do
    echo "Running ./main with parameter: $param"
    ./main $param 8 8
    echo "========================================"
    sleep 10
done

# third exp
echo "================= consumer experiment ======================="
parameters=(1 2 4 6 8 10 12 14 16 18)
for param in "${parameters[@]}"; do
    echo "Running ./main with parameter: $param"
    ./main 8 $param $param
    echo "========================================"
    sleep 10
done

# forth exp
echo "================= partition experiment ======================="
parameters=(1 2 4 6 8 10 12 14 16 18)
for param in "${parameters[@]}"; do
    echo "Running ./main with parameter: $param"
    ./main 8 8 $param
    echo "========================================"
    sleep 10
done

# fifth exp
echo "================= batch experiment ======================="
parameters=(1 10 50 100 500 1000 5000 10000 50000 100000 500000 1000000)
for param in "${parameters[@]}"; do
    echo "Running ./main with parameter: $param"
    ./main 8 8 8 $param
    echo "========================================"
    sleep 10
done
