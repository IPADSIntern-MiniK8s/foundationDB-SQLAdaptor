#ifndef CACHE_MANAGER_H_
#define CACHE_MANAGER_H_

#include <ctime>
#include <vector>
#include <thread>
#include <tuple>
#include "RedisUtils.h"
#include "../log/LogManager.h"

///@brief manage the data which is waiting for write into fdb
///@note only support one tag set
class CacheManager {
private:
    RedisUtils redis_utils_;
    LogManager log_manager_;
    ///! the start timestamp of each round
    static uint64_t start_timestamp_;
    ///! record the timestamp delta for each tag set
    std::vector<std::vector<uint64_t>> timestamp_delta_group_;
    ///! record tag value range 
    ///@note in our case, it is car count
    ///@note it is assign by user, hash to different timestamp_delta_group
    int tag_count_;
    ///! the bound to trigger flush
    int length_upper_bound_;

    ///@brief help determine the new timestamp after flush
    void UpdateStartTimestamp();

public:
    CacheManager(int tag_count = 4, int length_upper_bound = 120);

    void FlushCache();

    bool WriteToCache(const std::string &data, std::string &timestamp, int tag_key);

};

#endif