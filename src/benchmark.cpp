#include <lsm_tree.hh>
#include <iostream>
#include <map>
#include <sstream>
#include <fstream>
#include <vector>
#include <thread>
#include <chrono>

using namespace std;

void runCommand(const string& line, LSMTree* lsmTree) {
    istringstream iss(line);
    char command;
    int key, val;

    string fileName;
    iss >> command;

    switch (command) {
        case 'p':
            iss >> key >> val;
            lsmTree->putData({key, val, false});
            break;
        case 'g': {
            iss >> key;
            // Assuming get method prints the value or result
            std::optional<DataPair> data = lsmTree->getData(key);
            if (data.has_value()) {
                cout << "Get: Key = " << key << ", Value = " << data.value().value_ << endl;
            } else {
                cout << "boo" << endl;
            }
            break;
        }
        case 'd':
            iss >> key;
            lsmTree->deleteData(key);
            cout << "Deleted: " << key << endl;
            break;
        default:
            cout << "Unknown: " << command << endl;
    }
    // std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

void loadFileCommands(const string& filePath, LSMTree* db) {
    ifstream file(filePath);
    string line;

    if (!file.is_open()) {
        cout << "can't open " << filePath << endl;
        return;
    }

    while (getline(file, line)) {
        runCommand(line, db);
    }
    file.close();
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        cout << "Usage: " << argv[0] << " <path_to_benchmark_file>" << endl;
        return 1;
    }

    std::string workload_filename = "./experiments/" + std::string(argv[1]);
    std::cout << "Loading workload from: " << workload_filename << std::endl;

    // Initialize the LSM Tree
    auto start = std::chrono::high_resolution_clock::now();
    LSMTree lsmTree("benchmark_db");
    loadFileCommands("./experiments/put/1000MB_put.txt", &lsmTree);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << "Initial load time for 1000MB_put: " << duration << " seconds" << std::endl;

    loadFileCommands(workload_filename, &lsmTree);
    // Final duration after loading the workload
    auto end_get = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::seconds>(end_get - end).count();
    std::cout << "Total time to get 10MB data points from workload: " 
              << duration << " seconds" << std::endl;
    return 0;
}

// Start timer
auto start = std::chrono::high_resolution_clock::now();