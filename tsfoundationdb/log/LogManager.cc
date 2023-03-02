#include "LogManager.h"

LogManager::LogManager(int file_count, const std::string &file_name): 
    file_count_(file_count), file_name_("logs/" + file_name) {
    // rotate log file, each file is 5MB
    logger_ = spdlog::rotating_logger_mt("tsdb_logger", file_name_, 1048576 * 5, 5);
    // set the log format
    // auto formatter = std::make_unique<spdlog::pattern_formatter>();
    logger_->set_pattern("[%Y %m %d %T] %v");
    // logger_->set_pattern(">>>>>>>>> %H:%M:%S %z %v <<<<<<<<<");
    // logger_->set_formatter(std::move(formatter));
}


void LogManager::WriteToLog(const std::string &data, LOG_TYPE type) {
    logger_->info("{}{}", static_cast<int>(type), data.substr(2));
    logger_->flush();
}


int LogManager::FindMostRecentLog() {
    // TODO: the true file name may need more test
    int file_id = 0;
    std::string line;
    std::string max_timestamp, cur_timestamp;
    std::size_t end_pos = 0;
    for (int i = 0; i < file_count_; ++i) {
        std::string file_name = file_name_;
        if (file_id != 0) {
            file_name += std::to_string(file_id);  
        }
        std::ifstream infile(file_name);
        if (infile) {
            std::getline(infile, line);
            end_pos = line.find(']');
            if (end_pos != std::string::npos) {
                cur_timestamp = line.substr(1, end_pos - 1);
                if (max_timestamp.empty() || max_timestamp < cur_timestamp) {
                    max_timestamp = cur_timestamp;
                    file_id = i;
                }
            }
        }
        infile.close();
    }
    return file_id;
}


std::vector<std::string> LogManager::RestoreLog(int n) {
    int file_begin_pos = FindMostRecentLog();
    std::string line;
    std::size_t end_pos = 0;

    // restore the latest n log files
    int begin_pos = (file_begin_pos + file_count_ - n + 1) % file_count_;
    int j = 0;
    std::vector<std::string> result;
    while (j < n) {
        int file_id = (begin_pos + j) % file_count_;
        ++j;
        std::string file_name = file_name_;
        if (file_id != 0) {
            file_name += std::to_string(file_id);
        }
        spdlog::debug("filename: {}", file_name);
        std::ifstream infile(file_name);
        if (!infile) {
            spdlog::info("[LogManager] the corresponding log file is missing");
        }
        while (std::getline(infile, line)) {
            end_pos = line.find(']');
            if (end_pos != std::string::npos) {
                // TODO: write to tsdb directly may be a better choice
                // FIXME: according to debug result
                result.emplace_back("\n\n" + line.substr(end_pos + 3));
            } else {
                spdlog::info("[LogManager] illegal line");
            }
        }
        infile.close();
    }
    return result;
}


std::vector<std::string> LogManager::RestoreLog(const std::string &timestamp) {
    int file_id = 0, last_file = 0;
    std::string line;
    std::string min_timestamp, cur_timestamp;
    std::size_t end_pos = 0;
    
    for (int i = 0; i < file_count_; ++i) {
        std::string file_name = file_name_;
        if (i != 0) {
            file_name += std::to_string(i);  
        }
        std::ifstream infile(file_name);
        // std::ifstream infile(file_name_ + std::to_string(i));
        if (infile) {
            last_file = i;
            std::getline(infile, line);
            end_pos = line.find(']');
            if (end_pos != std::string::npos) {
                cur_timestamp = line.substr(1, end_pos - 1);
                if (timestamp < cur_timestamp) {
                    if (min_timestamp.empty() || min_timestamp > cur_timestamp) {
                        min_timestamp = cur_timestamp;
                        file_id = (i - 1 + file_count_) % file_count_;
                    }
                }
            } else {
                spdlog::info("[LogManager] illegal line, the line content: {}", line);
            }
        }
        infile.close();
    }

    // read the log after the specific timestamp
    int j = 0;
    std::vector<std::string> result;
    bool finished = false;
    
    // the wanted part only exist in the last log file
    if (min_timestamp.empty()) {
        file_id = last_file;
    }

    int begin_file_id = file_id; 
    while (j < file_count_ && !finished) {
        file_id = file_id % file_count_;
        ++j;
        std::string file_name = file_name_;
        if (file_id != 0) {
            file_name += std::to_string(file_id);  
        }   
        std::ifstream infile(file_name);
        // std::ifstream infile(file_name_ + std::to_string(file_id));
        if (!infile) {
            spdlog::info("[LogManager] the corresponding log file {} is missing", file_id);
            continue;
        }
        while (std::getline(infile, line)) {
            end_pos = line.find(']');
            if (end_pos != std::string::npos) {
                // TODO: write to tsdb directly may be a better choice
                cur_timestamp = line.substr(1, end_pos - 1);
                if (cur_timestamp < timestamp) {
                    spdlog::debug("[LogManager] the cur_timestamp: {}, the timestamp: {}", cur_timestamp, timestamp);
                    if (file_id == begin_file_id) {
                        continue;
                    } else {
                        // read end
                        break;
                    }
                } 
                result.emplace_back(line.substr(end_pos + 1));
            }
        }
        infile.close();
        ++file_id;
    }
    return result;
}
