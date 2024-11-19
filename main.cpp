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
int NUM_PRODUCERS = 64;
int NUM_CONSUMERS = 64;
int NUM_PARTITIONS = 64;
int NUM_PRODUCERS_FINISHED = 0;
pthread_mutex_t producersFinishedMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t partitionCollectorMutex = PTHREAD_MUTEX_INITIALIZER;

int PRODUCER_BATCH_SIZE = 1000;
int CONSUMER_BATCH_SIZE = 1000;
std::vector<int> partitionNum(NUM_PARTITIONS, 0);
std::vector<int> consumerNum(NUM_CONSUMERS, 0);
std::chrono::duration<double> waitDuration;

std::vector<std::string> globalLogs;

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

struct ProducerArg {
    MessageQueue *mq;
    int producerIndex;
};

struct ConsumerArg {
    MessageQueue *mq;
    int consumerIndex;
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
    size_t chunkSize = totalLogs / NUM_PRODUCERS;
    size_t startIndex = producerIndex * chunkSize;
    size_t endIndex = std::min(startIndex + chunkSize, totalLogs);
    return std::vector<std::string>(globalLogs.begin() + startIndex, globalLogs.begin() + endIndex);
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
        // pthread_mutex_lock(&partitionCollectorMutex);
        // partitionNum[partitionIndex] += dataBatch.size();
        // pthread_mutex_unlock(&partitionCollectorMutex);
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
        while (partition.queue.size() - partition.consumerIndex < CONSUMER_BATCH_SIZE) {
            pthread_mutex_lock(&producersFinishedMutex);
            if (NUM_PRODUCERS_FINISHED == NUM_PRODUCERS) {
                // std::cout << "escape" << std::endl;
                if (partition.consumerIndex < partition.queue.size()) {
                    pthread_mutex_unlock(&producersFinishedMutex);
                    break;
                }
                pthread_mutex_unlock(&producersFinishedMutex);
                pthread_mutex_unlock(&partition.indexMutex);
                return batch;
            }
            pthread_mutex_unlock(&producersFinishedMutex);
            // std::cout << "busy waiting" << std::endl;
            auto waitStart = std::chrono::high_resolution_clock::now();
            pthread_cond_wait(&partition.cond_consume, &partition.indexMutex);
            auto waitEnd = std::chrono::high_resolution_clock::now();
            waitDuration += (waitEnd - waitStart);
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
    ProducerArg *producerArg = static_cast<ProducerArg *>(arg);
    MessageQueue *mq = producerArg->mq;
    MessageQueueManager manager(mq->partitions);

    size_t index = producerArg->producerIndex;
    size_t totalLogs = globalLogs.size();
    size_t chunkSize = totalLogs / NUM_PRODUCERS;
    size_t startIndex = index * chunkSize;
    size_t endIndex = std::min(startIndex + chunkSize, totalLogs);
    if (index == NUM_PRODUCERS - 1) {
        endIndex = totalLogs;
    }
    // std::vector<std::string> logs = distributeLog(index);
    for (size_t i = startIndex; i < endIndex; i += PRODUCER_BATCH_SIZE) {
        std::vector<std::string> batch;
        for (size_t j = i; j < i + PRODUCER_BATCH_SIZE && j < endIndex; ++j) {
            batch.push_back(globalLogs[j]);
        }
        manager.pushBatch(batch);
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
        if (latestPartitionSize - latestPartitionIndex < CONSUMER_BATCH_SIZE) {
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

int main(int argc, char *argv[]) {
    NUM_PRODUCERS = std::stoi(argv[1]);
    NUM_CONSUMERS = std::stoi(argv[2]);
    NUM_PARTITIONS = std::stoi(argv[3]);
    if (argc > 4) {
        PRODUCER_BATCH_SIZE = std::stoi(argv[4]);
        CONSUMER_BATCH_SIZE = std::stoi(argv[4]);
    }
    std::cout << "number of producers: " << NUM_PRODUCERS << "\n";
    std::cout << "number of consumers: " << NUM_CONSUMERS << "\n";
    std::cout << "number of partitions: " << NUM_PARTITIONS << "\n";
    std::cout << "producer batch size: " << PRODUCER_BATCH_SIZE << "\n";
    std::cout << "consumer batch size: " << CONSUMER_BATCH_SIZE << "\n";

    std::vector<Partition> partitions(NUM_PARTITIONS);
    for (auto &partition: partitions) {
        partition.queueMutex = PTHREAD_MUTEX_INITIALIZER;
        partition.indexMutex = PTHREAD_MUTEX_INITIALIZER;
        partition.cond_produce = PTHREAD_COND_INITIALIZER;
        partition.cond_consume = PTHREAD_COND_INITIALIZER;
    }

    MessageQueue messageQueue{partitions};
    auto loadingStart = std::chrono::high_resolution_clock::now();
    loadLogs("combined_log.log");
    auto loadingEnd = std::chrono::high_resolution_clock::now();

    auto start = std::chrono::high_resolution_clock::now();
    std::vector<pthread_t> producerThreads(NUM_PRODUCERS);
    std::vector<ProducerArg> producerArgs(NUM_PRODUCERS);
    for (int i = 0; i < NUM_PRODUCERS; i++) {
        producerArgs[i].mq = &messageQueue;
        producerArgs[i].producerIndex = i;
        pthread_create(&producerThreads[i], nullptr, producer, &producerArgs[i]);
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

    std::chrono::duration<double> loadingElapsed = loadingEnd - loadingStart;
    std::chrono::duration<double> elapsed = end - start;
    size_t log_size = globalLogs.size();
    double throughput = log_size / elapsed.count();
    double latency = elapsed.count() / log_size;
    std::cout << "Serial Elapsed Time (loading data): " << loadingElapsed.count() << " seconds\n";
    std::cout << "Parallel Elapsed Time: " << elapsed.count() << " seconds\n";
    std::cout << "Overall Elapsed Time: " << loadingElapsed.count() + elapsed.count() << " seconds\n";
    std::cout << "Throughput: " << throughput << " operations/second\n";
    std::cout << "Latency: " << latency << " seconds/operation\n";
    int count = 0;
    for (auto &partition: partitions) {
        count += partition.queue.size();
    }
    std::cout << "total has " << count << " elements.\n";
    count = 0;
    for (size_t i = 0; i < consumerNum.size(); ++i) {
        // std::cout << "Consumer " << i << " completes " << consumerNum[i] << " elements.\n";
        count += consumerNum[i];
    }
    std::cout << "total has " << count << " elements.\n";
    std::cout << "total time spent on waiting " << waitDuration.count() << " seconds\n";
    return 0;
}
