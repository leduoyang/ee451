#include <iostream>
#include <fstream>
#include <queue>
#include <vector>
#include <string>
#include <pthread.h>
#include <chrono>

/* # of producers
 * # of consumers
 * # of partitions
 */
const int NUM_PRODUCERS = 10;
const int NUM_CONSUMERS = 4;
const int NUM_PARTITIONS = 4;
int NUM_PRODUCERS_FINISHED = 0;
pthread_mutex_t producersFinishedMutex = PTHREAD_MUTEX_INITIALIZER;

const int PRODUCER_BATCH_SIZE = 100;
const int CONSUMER_BATCH_SIZE = 100;

struct Partition {
    std::queue<std::string> queue;
    pthread_mutex_t mutex;
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

    bool push(const std::string &data) {
        size_t partitionIndex = std::hash<std::string>{}(data) % partitions.size();
        Partition &partition = partitions[partitionIndex];

        pthread_mutex_lock(&partition.mutex);
        partition.queue.push(data);
        pthread_cond_signal(&partition.cond_consume);
        // pthread_cond_broadcast(&partition.cond_consume);
        pthread_mutex_unlock(&partition.mutex);
        return true;
    }

    void pushBatch(const std::vector<std::string> &dataBatch) {
        size_t partitionIndex = std::hash<std::string>{}(dataBatch[0]) % partitions.size();
        Partition &partition = partitions[partitionIndex];
        pthread_mutex_lock(&partition.mutex);
        for (const auto &log: dataBatch) {
            partition.queue.push(log);
        }
        pthread_cond_signal(&partition.cond_consume);
        pthread_mutex_unlock(&partition.mutex);
    }

    std::string retrieve(int partitionIndex) {
        std::string data = "";
        Partition &partition = partitions[partitionIndex];
        pthread_mutex_lock(&partition.mutex);
        while (partition.queue.empty()) {
            pthread_mutex_lock(&producersFinishedMutex);
            if (NUM_PRODUCERS_FINISHED == NUM_PRODUCERS) {
                std::cout << "escape" << std::endl;
                pthread_mutex_unlock(&producersFinishedMutex);
                pthread_mutex_unlock(&partition.mutex);
                return "";
            }
            pthread_mutex_unlock(&producersFinishedMutex);
            pthread_cond_wait(&partition.cond_consume, &partition.mutex);
            std::cout << "busy waiting" << std::endl;
        }
        if (!partition.queue.empty()) {
            data = partition.queue.front();
            partition.queue.pop();
        }
        pthread_mutex_unlock(&partition.mutex);
        return data;
    }

    std::vector<std::string> retrieveBatch(int partitionIndex) {
        std::vector<std::string> batch;
        Partition &partition = partitions[partitionIndex];
        pthread_mutex_lock(&partition.mutex);
        while (partition.queue.empty()) {
            pthread_mutex_lock(&producersFinishedMutex);
            if (NUM_PRODUCERS_FINISHED == NUM_PRODUCERS) {
                std::cout << "escape" << std::endl;
                pthread_mutex_unlock(&producersFinishedMutex);
                pthread_mutex_unlock(&partition.mutex);
                return batch;
            }
            pthread_mutex_unlock(&producersFinishedMutex);
            pthread_cond_wait(&partition.cond_consume, &partition.mutex);
            std::cout << "busy waiting" << std::endl;
        }
        if (!partition.queue.empty()) {
            for (int i = 0; i < CONSUMER_BATCH_SIZE && !partition.queue.empty(); ++i) {
                batch.push_back(partition.queue.front());
                partition.queue.pop(); // Consumers still pop from the queue in this version
            }
        }
        pthread_mutex_unlock(&partition.mutex);
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
    std::cout << "consumer in" << std::endl;
    ConsumerArg *consumerArg = static_cast<ConsumerArg *>(arg);
    MessageQueue *mq = consumerArg->mq;
    MessageQueueManager manager(mq->partitions);

    int partitionIndex = consumerArg->consumerIndex % mq->partitions.size();
    while (true) {
        std::vector<std::string> batch = manager.retrieveBatch(partitionIndex);
        if (batch.empty()) {
            break;
        }
        for (const auto &log: batch) {
            std::cout << "Consumer " << consumerArg->consumerIndex << " processed log: " << log << std::endl;
        }
    }
    return nullptr;
}

int main() {
    // initialize a message queue with specific number of partitions based on the number of consumers
    std::vector<Partition> partitions(NUM_PARTITIONS);
    for (auto &partition: partitions) {
        partition.mutex = PTHREAD_MUTEX_INITIALIZER;
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
    return 0;
}
