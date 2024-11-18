#include <iostream>
#include <fstream>
#include <queue>
#include <vector>
#include <string>
#include <pthread.h>
#include <chrono>
#include <thread>

/* # of producers
 * # of consumers
 * # of partitions
 */
const int NUM_PRODUCERS = 32;
const int NUM_CONSUMERS = 6;
const int NUM_PARTITIONS = 6;
int NUM_PRODUCERS_FINISHED = 0;
pthread_mutex_t producersFinishedMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t partitionCollectorMutex = PTHREAD_MUTEX_INITIALIZER;
int NUM_LOGS = 30000000;

const int PRODUCER_BATCH_SIZE = 100;
const int CONSUMER_BATCH_SIZE = 100;
std::vector<int> partitionNum(NUM_PARTITIONS, 0);
std::vector<int> consumerNum(NUM_CONSUMERS, 0);
int busyWaitingNum = 0;

struct Partition {
    size_t consumerIndex = 0;
    std::queue<std::string> queue;
    pthread_mutex_t queueMutex;
    pthread_mutex_t indexMutex;
    pthread_cond_t cond_produce;
    pthread_cond_t cond_consume;
};

struct MessageQueue {
    std::vector<Partition> &partitions;
};

struct ConsumerArg {
    MessageQueue *mq;
    int consumerIndex;
};

std::vector<std::string> loadLogs(const std::string &filename, const int num) {
    std::vector<std::string> logs;
    std::ifstream file(filename);
    if (!file) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return logs;
    }
    int count = 0;
    std::string line;
    while (std::getline(file, line)) {
        logs.push_back(line);
        count += 1;
        if (count >= num) {
            break;
        }
    }
    file.close();
    return logs;
}

class MessageQueueManager {
    std::vector<Partition> &partitions;

public:
    MessageQueueManager(std::vector<Partition> &partitions) : partitions(partitions) {
    }

    void pushBatch(const std::vector<std::string> &dataBatch) {
        // Find the partition with the smallest queue size
        size_t partitionIndex = std::hash<std::string>{}(dataBatch[0]) % partitions.size();
        size_t minQueueSize = std::numeric_limits<size_t>::max();
        for (size_t i = 0; i < partitions.size(); ++i) {
            // pthread_mutex_lock(&partitions[i].queueMutex);
            size_t currentQueueSize = partitions[i].queue.size();
            // pthread_mutex_unlock(&partitions[i].queueMutex);
            if (currentQueueSize < minQueueSize) {
                minQueueSize = currentQueueSize;
                partitionIndex = i;
            }
        }

        // size_t partitionIndex = std::hash<std::string>{}(dataBatch[0]) % partitions.size();
        pthread_mutex_lock(&partitionCollectorMutex);
        partitionNum[partitionIndex] += dataBatch.size();
        pthread_mutex_unlock(&partitionCollectorMutex);
        Partition &partition = partitions[partitionIndex];
        pthread_mutex_lock(&partition.queueMutex);
        for (const auto &log: dataBatch) {
            partition.queue.push(log);
        }
        pthread_cond_signal(&partition.cond_consume);
        pthread_mutex_unlock(&partition.queueMutex);
    }

    std::vector<std::string> retrieveBatchByIndex(int partitionIndex) {
        std::vector<std::string> batch;
        Partition &partition = partitions[partitionIndex];
        pthread_mutex_lock(&partition.indexMutex);
        while (partition.consumerIndex == partition.queue.size()) {
            pthread_mutex_lock(&producersFinishedMutex);
            if (NUM_PRODUCERS_FINISHED == NUM_PRODUCERS) {
                // std::cout << "escape" << std::endl;
                pthread_mutex_unlock(&producersFinishedMutex);
                pthread_mutex_unlock(&partition.indexMutex);
                return batch;
            }
            pthread_mutex_unlock(&producersFinishedMutex);
            // std::cout << "busy waiting" << std::endl;
            busyWaitingNum += 1;
            pthread_cond_wait(&partition.cond_consume, &partition.indexMutex);
        }
        size_t availableLogs = partition.queue.size() - partition.consumerIndex;
        size_t logsToRetrieve = std::min(availableLogs, static_cast<size_t>(CONSUMER_BATCH_SIZE));
        partition.consumerIndex += logsToRetrieve;
        pthread_mutex_unlock(&partition.indexMutex);
        for (int i = 0; i < logsToRetrieve; ++i) {
            batch.push_back(partition.queue.front());
        }
        return batch;
    }

    void broadcast() {
        // std::cout << "broadcast in" << std::endl;
        pthread_mutex_lock(&producersFinishedMutex);
        if (NUM_PRODUCERS_FINISHED == NUM_PRODUCERS) {
            for (auto &partition: partitions) {
                pthread_cond_broadcast(&partition.cond_consume);
            }
        }
        pthread_mutex_unlock(&producersFinishedMutex);
    }
};

void *producer(void *arg) {
    MessageQueue *mq = static_cast<MessageQueue *>(arg);
    MessageQueueManager manager(mq->partitions);
    std::vector<std::string> logs = loadLogs("combined_log.log", NUM_LOGS / NUM_PRODUCERS);
    for (size_t i = 0; i < logs.size(); i += PRODUCER_BATCH_SIZE) {
        std::vector<std::string> batch;
        for (size_t j = i; j < i + PRODUCER_BATCH_SIZE && j < logs.size(); ++j) {
            batch.push_back(logs[j]);
        }
        manager.pushBatch(batch); // Push the entire batch at once
    }
    pthread_mutex_lock(&producersFinishedMutex);
    NUM_PRODUCERS_FINISHED += 1;
    pthread_mutex_unlock(&producersFinishedMutex);
    manager.broadcast();
    return nullptr;
}

void *consumer(void *arg) {
    ConsumerArg *consumerArg = static_cast<ConsumerArg *>(arg);
    MessageQueue *mq = consumerArg->mq;
    MessageQueueManager manager(mq->partitions);

    int partitionIndex = consumerArg->consumerIndex % mq->partitions.size();
    while (true) {
        std::vector<std::string> batch = manager.retrieveBatchByIndex(partitionIndex);
        if (batch.empty()) {
            break;
        }
        for (const auto &log: batch) {
            for (const char &ch: log) {
                // Process each character 'ch' in 'log'
            }
        }
        consumerNum[consumerArg->consumerIndex] += batch.size();

        size_t latestPartitionSize = mq->partitions[partitionIndex].queue.size();
        size_t latestPartitionIndex = mq->partitions[partitionIndex].consumerIndex;
        size_t maxAvailableLogs = latestPartitionSize > latestPartitionIndex
                                      ? latestPartitionSize - latestPartitionIndex
                                      : 0;
        // Re-balance: Find a better partition
        if (latestPartitionSize - latestPartitionIndex < batch.size()) {
            size_t nextPartitionIndex = partitionIndex;
            for (size_t i = 0; i < mq->partitions.size(); i++) {
                size_t candidateIndex = (partitionIndex + i) % mq->partitions.size();
                size_t candidateQueueSize = mq->partitions[candidateIndex].queue.size();
                size_t candidateQueueIndex = mq->partitions[candidateIndex].consumerIndex;
                size_t candidateAvailableLogs = candidateQueueSize > candidateQueueIndex
                                                    ? candidateQueueSize - candidateQueueIndex
                                                    : 0;
                if (candidateAvailableLogs > maxAvailableLogs) {
                    nextPartitionIndex = candidateIndex;
                    maxAvailableLogs = candidateAvailableLogs;
                }
            }
            partitionIndex = nextPartitionIndex;
        }
    }
    return nullptr;
}

int main() {
    std::vector<double> elapsedTimes;
    std::vector<double> throughputs;
    std::vector<double> latencies;
    for (int num = 1000000; num <= 10000000; num += 1000000) {
        NUM_LOGS = num;
        std::cout << "target number of logs " << NUM_LOGS << std::endl;

        // initialize a message queue with specific number of partitions based on the number of consumers
        std::vector<Partition> partitions(NUM_PARTITIONS);
        for (auto &partition: partitions) {
            partition.queueMutex = PTHREAD_MUTEX_INITIALIZER;
            partition.indexMutex = PTHREAD_MUTEX_INITIALIZER;
            partition.cond_produce = PTHREAD_COND_INITIALIZER;
            partition.cond_consume = PTHREAD_COND_INITIALIZER;
        }
        MessageQueue messageQueue{partitions};

        auto start = std::chrono::high_resolution_clock::now();
        std::vector<pthread_t> producerThreads(NUM_PRODUCERS);
        for (int i = 0; i < NUM_PRODUCERS; i++) {
            pthread_create(&producerThreads[i], nullptr, producer, &messageQueue);
        }

        std::vector<pthread_t> consumerThreads(NUM_CONSUMERS);
        std::vector<ConsumerArg> consumerArgs(NUM_CONSUMERS);
        for (int i = 0; i < NUM_CONSUMERS; i++) {
            consumerArgs[i].mq = &messageQueue;
            consumerArgs[i].consumerIndex = i;
            pthread_create(&consumerThreads[i], nullptr, consumer, &consumerArgs[i]);
        }

        for (int i = 0; i < NUM_PRODUCERS; i++) {
            pthread_join(producerThreads[i], nullptr);
        }
        for (int i = 0; i < NUM_CONSUMERS; i++) {
            pthread_join(consumerThreads[i], nullptr);
        }
        auto end = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> elapsed = end - start;
        elapsedTimes.push_back(elapsed.count());
        std::cout << "Total execution time: " << elapsed.count() << " seconds" << std::endl;
        // compute throughput
        double throughput = NUM_LOGS / elapsed.count();
        double latency = elapsed.count() / NUM_LOGS;
        throughputs.push_back(throughput);
        latencies.push_back(latency);
        // compute latency
        // int count = 0;
        // for (size_t i = 0; i < partitionNum.size(); ++i) {
        //     std::cout << "Partition " << i << " has " << partitionNum[i] << " elements.\n";
        //     count += partitionNum[i];
        // }
        // std::cout << "total has " << count << " elements.\n";
        // count = 0;
        // for (size_t i = 0; i < consumerNum.size(); ++i) {
        //     std::cout << "Consumer " << i << " completes " << consumerNum[i] << " elements.\n";
        //     count += consumerNum[i];
        // }
        // std::cout << "total has " << count << " elements.\n";
        // std::cout << "there are " << busyWaitingNum << " busy waiting.\n";
    }
    std::cout << "Elapsed times for all iterations:" << std::endl;
    for (size_t i = 0; i < elapsedTimes.size(); ++i) {
        std::cout << "Execution Time: " << elapsedTimes[i] << " ms\n";
        std::cout << "Throughput: " << throughputs[i] << " operations/second\n";
        std::cout << "Latency: " << latencies[i] << " seconds/operation\n\n";
    }
    return 0;
}
