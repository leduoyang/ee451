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
const int NUM_PRODUCERS = 1;
const int NUM_CONSUMERS = 1;
const int NUM_PARTITIONS = 1;
int NUM_PRODUCERS_FINISHED = 0;
pthread_mutex_t producersFinishedMutex = PTHREAD_MUTEX_INITIALIZER;

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
    std::vector<Partition>& partitions;

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
    for (const auto &log: logs) {
        std::cout << log << std::endl;
        manager.push(log);
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
        std::string data = manager.retrieve(partitionIndex);
        if (data.empty()) {
            break;
        }
        std::cout << "Consumer " << consumerArg->consumerIndex << " processed log: " << data << std::endl;
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
