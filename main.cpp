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

const int PRODUCER_BATCH_SIZE = 100;
const int CONSUMER_BATCH_SIZE = 100;
std::vector<int> partitionNum(NUM_PARTITIONS, 0);

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

std::vector<std::string> loadLogs(const std::string &filename) {
    std::vector<std::string> logs;
    std::ifstream file(filename);
    if (!file) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return logs;
    }
    std::string line;
    while (std::getline(file, line)) {
        logs.push_back(line);
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
        size_t partitionIndex = std::hash<std::string>{}(dataBatch[0]) % partitions.size();
        pthread_mutex_lock(&partitionCollectorMutex);
        partitionNum[partitionIndex] += dataBatch.size();
        pthread_mutex_unlock(&partitionCollectorMutex);
        std::cout << partitionIndex << std::endl;
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
                std::cout << "escape" << std::endl;
                pthread_mutex_unlock(&producersFinishedMutex);
                pthread_mutex_unlock(&partition.indexMutex);
                return batch;
            }
            pthread_mutex_unlock(&producersFinishedMutex);
            std::cout << "busy waiting" << std::endl;
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
        std::cout << "broadcast in" << std::endl;
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
    std::vector<std::string> logs = loadLogs("apache.log");
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
            std::this_thread::sleep_for(std::chrono::microseconds(10));
            // std::cout << "Consumer " << consumerArg->consumerIndex << " processed log: " << log << std::endl;
        }
    }
    return nullptr;
}

int main() {
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
    std::cout << "Total execution time: " << elapsed.count() << " seconds" << std::endl;
    int count = 0;
    for (size_t i = 0; i < partitionNum.size(); ++i) {
        std::cout << "Partition " << i << " has " << partitionNum[i] << " elements.\n";
        count += partitionNum[i];
    }
    std::cout << "total has " << count << " elements.\n";
    return 0;
}
