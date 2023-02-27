#ifndef LOG_MANAGER_H_
#define LOG_MANAGER_H_

#include "spdlog/pattern_formatter.h"
#include "spdlog/spdlog.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "../metadata/MetaDataManager.h"

enum class LOG_TYPE {
    INSERT,
    DELETE,
};

///@brief Manage and implement write ahead log, where 
///@b spdlog is used as the underlying support
class LogManager {
private:
    int file_count_;
    std::string file_name_;
    std::shared_ptr<spdlog::logger> logger_;

    ///@brief find the most recently log
    int FindMostRecentLog();
    
public:
    LogManager(int file_count = 5, const std::string &file_name = "tsdb_log");

    void WriteToLog(const std::string &data, LOG_TYPE type = LOG_TYPE::INSERT);

    ///@brief restore the most recent log, the wanted log count is according to @param n
    std::vector<std::string> RestoreLog(int n = 1);

    ///@brief restore the log after a time point
    std::vector<std::string> RestoreLog(const std::string &timestamp);
};
#endif