#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <chrono>
#include <thread>


// Function to load logs from a file into a vector
std::vector<std::string> loadLogs(const std::string &filename, const int num) {
    std::vector<std::string> logs;
    std::ifstream file(filename);
    if (!file) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return logs;
    }
    std::string line;
    int count = 0;
    while (std::getline(file, line)) {
        logs.push_back(line);
        count += 1;
        if(count == num) {
          break;
        }
    }
    file.close();
    return logs;
}

// Function to process logs (simulated by printing the log)
void processLog(const std::string &log) {
    // Simulate processing the log (e.g., parsing, analysis, etc.)
//    std::cout << "Processed log: " << log << std::endl;
//    std::this_thread::sleep_for(std::chrono::microseconds(10));
    for (const char &ch : log) {
        // Process each character 'ch' in 'log'
    }
}

int main() {
    std::vector<double> elapsedTimes;
    std::vector<double> throughputs;
    std::vector<double> latencies;

    for (int num = 1000000; num <= 10000000; num += 1000000) {
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<std::string> logs = loadLogs("apache.log", num);
        // Process each log sequentially
        for (const auto &log : logs) {
            processLog(log);
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsed = end - start;
        elapsedTimes.push_back(elapsed.count());
        std::cout << "Total execution time: " << elapsed.count() << " seconds" << std::endl;
        double throughput = num / elapsed.count();
        double latency = elapsed.count() / num;
        throughputs.push_back(throughput);
        latencies.push_back(latency);
    }
    std::cout << "Elapsed times for all iterations:" << std::endl;
    for (size_t i = 0; i < elapsedTimes.size(); ++i) {
        std::cout << "Execution Time: " << elapsedTimes[i] << " ms\n";
        std::cout << "Throughput: " << throughputs[i] << " operations/second\n";
        std::cout << "Latency: " << latencies[i] << " seconds/operation\n\n";
    }
    return 0;
}