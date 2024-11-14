#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <chrono>
#include <thread>


// Function to load logs from a file into a vector
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
    const int NUM_PRODUCERS = 32;
    // Start the timer
    auto start = std::chrono::high_resolution_clock::now();

    int count = 0;
    // Load logs from the file
    for(int i = 0; i < NUM_PRODUCERS; i++) {
        std::vector<std::string> logs = loadLogs("apache.log");

        // Process each log sequentially
        for (const auto &log : logs) {
            processLog(log);
            count += 1;
        }
    }
    std::cout << "total has " << count << " elements.\n";

    // End the timer
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;

    // Print total execution time
    std::cout << "Total execution time (serial): " << elapsed.count() << " seconds" << std::endl;

    return 0;
}