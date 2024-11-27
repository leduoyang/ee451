# Parallel Log Management

## Overview
The Parallel Log Management System is designed to efficiently handle large-scale log data. It leverages a custom-built message queue to optimize log ingestion, processing, and storage using data parallelism. By focusing on scalability and efficient resource utilization, this project aims to address the challenges of log management while reducing synchronization cost and lock contention.

## Features
- Custom Message Queue: Supports multiple producers, consumers, and partitions for efficient data handling.
- Data Parallelism: Processes logs in parallel batches for enhanced performance.
- Partitioning with load balancing: Assigns log entries to partitions for balanced workload distribution.
- In-Memory Processing: Capable of handling large scale log files for real-time processing.

## Usage
1. Clone the repository
```
git clone https://github.com/leduoyang/ee451.git  
cd ee451
```
2. Build the project:
```
make
```
3. run the script:
```
./main ${number_of_producer} ${number_of_consumer} ${number_of_partition}
```
