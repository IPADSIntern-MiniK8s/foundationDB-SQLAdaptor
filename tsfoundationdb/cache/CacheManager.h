#ifndef CACHE_MANAGER_H_
#define CACHE_MANAGER_H_

#include <algorithm>
#include <ctime>
#include <memory>
#include <vector>
#include <thread>
#include <tuple>
#include <unordered_map>
#include "RedisUtils.h"
#include "../metadata/MetaDataManager.h"
#include "../protodata/FieldMessage.pb.h"
#include "../service/DataService.h"
#include "../storage/Storage.h"

///@brief manage the data which is waiting for write into fdb
///@note only support one tag set
class CacheManager {
private:
    RedisUtils redis_utils_;
    ///! the tool for operation on fdb
    Storage storage_tool_;
    ///! the start timestamp of each round
    uint64_t start_timestamp_;
    ///! record the timestamp delta for each tag set
    std::vector<std::vector<uint64_t>> timestamp_delta_group_;
    ///! record tag value range 
    ///@note in our case, it is car count
    ///@note it is assign by user, hash to different timestamp_delta_group
    int tag_range_;
    ///! the bound to trigger flush
    int length_upper_bound_;
    ///! the biggest group's size
    int cur_length_;

    ///@brief help determine the new timestamp after flush
    void UpdateStartTimestamp();

public:
    CacheManager(int tag_count = 4, int length_upper_bound = 120);

    void FlushCache();

    bool WriteToCache(const std::string &data, std::string &timestamp, int tag_key);

    bool TriggerFlush();

    ///@brief immediately read to fdb
    bool ImmediateStore(const std::string &timestamp, int tag_key, const std::string &data);
};

#endif