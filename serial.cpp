#include <iostream>
#include <fstream>
#include <queue>
#include <vector>
#include <string>
#include <chrono>
#include <limits>

/* # of partitions */
const int NUM_PARTITIONS = 1;
const int PRODUCER_BATCH_SIZE = 100;
const int CONSUMER_BATCH_SIZE = 100;

std::vector<int> partitionNum(NUM_PARTITIONS, 0);
std::vector<int> consumerNum(NUM_PARTITIONS, 0);

std::vector<std::string> globalLogs;

struct Partition {
    size_t consumerIndex = 0;
    std::queue<std::string> queue;
};

void loadLogs(const std::string &filename) {
    std::ifstream file(filename);
    if (!file) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return;
    }
    std::string line;
    while (std::getline(file, line)) {
        globalLogs.push_back(line);
    }
    file.close();
}

std::vector<std::string> distributeLog(size_t producerIndex) {
    size_t totalLogs = globalLogs.size();
    size_t chunkSize = (totalLogs + NUM_PARTITIONS - 1) / NUM_PARTITIONS;
    size_t startIndex = producerIndex * chunkSize;
    size_t endIndex = std::min(startIndex + chunkSize, totalLogs);
    if (startIndex >= totalLogs) {
        return {}; // Return empty vector if startIndex is out of bounds
    }
    return std::vector<std::string>(globalLogs.begin() + startIndex, globalLogs.begin() + endIndex);
}

class MessageQueueManager {
    std::vector<Partition> &partitions;

public:
    MessageQueueManager(std::vector<Partition> &partitions) : partitions(partitions) {
    }

    void pushBatch(const std::vector<std::string> &dataBatch) {
        if (dataBatch.empty()) return; // Ensure we do not push empty batches

        // Find the partition with the smallest queue size
        size_t partitionIndex = std::hash<std::string>{}(dataBatch[0]) % partitions.size();
        size_t minQueueSize = std::numeric_limits<size_t>::max();
        for (size_t i = 0; i < partitions.size(); ++i) {
            size_t currentQueueSize = partitions[i].queue.size();
            if (currentQueueSize < minQueueSize) {
                minQueueSize = currentQueueSize;
                partitionIndex = i;
            }
        }

        partitionNum[partitionIndex] += dataBatch.size();
        Partition &partition = partitions[partitionIndex];
        for (const auto &log: dataBatch) {
            partition.queue.push(log);
        }
    }

    std::vector<std::string> retrieveBatchByIndex(int partitionIndex) {
        std::vector<std::string> batch;
        Partition &partition = partitions[partitionIndex];

        size_t availableLogs = partition.queue.size() - partition.consumerIndex;
        size_t logsToRetrieve = std::min(availableLogs, static_cast<size_t>(CONSUMER_BATCH_SIZE));
        partition.consumerIndex += logsToRetrieve;

        for (int i = 0; i < logsToRetrieve; ++i) {
            if (!partition.queue.empty()) {
                batch.push_back(partition.queue.front());
                partition.queue.pop(); // Safely pop the queue
            }
        }
        return batch;
    }
};

int main(int argc, char *argv[]) {
    // Initialize a message queue with a specific number of partitions
    std::vector<Partition> partitions(NUM_PARTITIONS);
    MessageQueueManager manager(partitions);
    loadLogs("combined_log.log");
    if (argc == 2) {
        int NUM_LOGS = std::stoi(argv[1]);
        if (NUM_LOGS < globalLogs.size()) {
            globalLogs.resize(NUM_LOGS);
        }
    }
    auto start = std::chrono::high_resolution_clock::now();

    // Sequentially process logs (producer + consumer logic combined)

    // Producer logic: distribute logs to partitions
    for (int i = 0; i < NUM_PARTITIONS; ++i) {
        std::vector<std::string> logs = distributeLog(i);
        for (size_t j = 0; j < logs.size(); j += PRODUCER_BATCH_SIZE) {
            std::vector<std::string> batch;
            for (size_t k = j; k < j + PRODUCER_BATCH_SIZE && k < logs.size(); ++k) {
                batch.push_back(logs[k]);
            }
            manager.pushBatch(batch);
        }
    }

    // Consumer logic: retrieve and process logs from partitions
    for (int i = 0; i < NUM_PARTITIONS; ++i) {
        while (true) {
            std::vector<std::string> batch = manager.retrieveBatchByIndex(i);
            if (batch.empty()) {
                break; // No more logs to process
            }
            for (const auto &log: batch) {
                for (const char &ch: log) {
                    // Process each character 'ch' in 'log'
                }
            }
            consumerNum[i] += batch.size();
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    size_t log_size = globalLogs.size();
    double throughput = log_size / elapsed.count();
    double latency = elapsed.count() / log_size;
    std::cout << "Elapsed Time: " << elapsed.count() << " seconds\n";
    std::cout << "Throughput: " << throughput << " operations/second\n";
    std::cout << "Latency: " << latency << " seconds/operation\n";
    return 0;
}
