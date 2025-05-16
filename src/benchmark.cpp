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
        case 'd': {
            iss >> key;
            lsmTree->deleteData(key);
            cout << "Deleted: " << key << endl;
            break;
        }
        case 'r': {
            int low, high;
            iss >> low >> high;
            vector<DataPair> range = lsmTree->rangeData(low, high);
            cout << "Range: " << low << " to " << high << ", lenth: " << range.size() << endl;
            // for (const auto& data : range) {
            //     cout << data.key_ << ":" << data.value_ << " ";
            // }
            cout << endl;
            break;
        }
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

void loadFileCommandsLoad(const string& file_path, LSMTree* db) {
    // open file in binary, and every 4 bytes is a key, and 4 more bytes is a value
    std::ifstream file(file_path, std::ios::binary | std::ios::ate);

    if (!file.is_open()) {
        cout << "can't open" << file_path << endl;
    }
    std::streamsize file_size = file.tellg();
    file.seekg(0, std::ios::beg);

    if (file_size == 0) {
        file.close();
        cout << "File is empty: " << file_path << endl;
    }

    // file_size must be a multiple of 8 (4+4 bytes key/val pairs)
    if (file_size % 8 != 0) {
        file.close();
        cout << "File size is not a multiple of 8: " << file_path << endl;
    }

    int key_from_file;
    int value_from_file;
    unsigned long items_processed_from_file = 0;
    unsigned long items_successfully_put = 0;
    bool read_error_occurred = false;

    // read key/val pairs from file
    while (file.good() && (items_processed_from_file < (unsigned long)(file_size / 8))) {
        // Read 4 bytes for the key
        if (!file.read(reinterpret_cast<char*>(&key_from_file), sizeof(key_from_file))) {
            if (!file.eof()) {
                read_error_occurred = true;
            }
            break;
        }
        // Read 4 bytes for the value
        if (!file.read(reinterpret_cast<char*>(&value_from_file), sizeof(value_from_file))) {
            if (!file.eof()) { // If not EOF, it's an actual read error for value
                read_error_occurred = true;
            }
            // This implies we read a key but couldn't read its corresponding value
            cout << "[SERVER] LOAD: Read key but failed to read value from '" << file_path 
                 << "'. File might be truncated or corrupt.\n";
        }
        // track items processed
        items_processed_from_file++;
        DataPair data_to_put(key_from_file, value_from_file, false);

        if (db->putData(data_to_put)) {
            items_successfully_put++;
            cout << "loaded " << key_from_file << ":" << value_from_file << endl;
        } else {
            cout << "[SERVER] LOAD: putData failed for key " << key_from_file << ", value " 
                 << value_from_file << " from file '" << file_path << "'. Continuing.\n";
        }
    } 

    if (read_error_occurred) {
        file.close();
        char err_buf[FILENAME_MAX + 128];
        cout << "read error" << endl;
    }

    file.close();
    cout << "loaded " << items_processed_from_file << " pairs, successfully put " 
         << items_successfully_put << " pairs into LSM Tree from '" << file_path << "'." << endl;
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
    // loadFileCommands("./experiments/put/1000MB_put.txt", &lsmTree);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    // std::cout << "Initial load time for 1000MB_put: " << duration << " seconds" << std::endl;

    // if the workload filename includes "load":
    if (workload_filename.find("load") != std::string::npos) {
        loadFileCommandsLoad(workload_filename, &lsmTree);
    } else {
        loadFileCommands(workload_filename, &lsmTree);
    }

    // Final duration after loading the workload
    auto end_get = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::seconds>(end_get - end).count();
    std::cout << "Total time to perform workload: " << duration << " seconds" 
              << ". Workload name: " << workload_filename << std::endl;
    return 0;
}

// Start timer
auto start = std::chrono::high_resolution_clock::now();