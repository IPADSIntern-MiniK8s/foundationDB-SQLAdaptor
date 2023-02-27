#include "CacheManager.h"


CacheManager::CacheManager(int tag_count = 4, int length_upper_bound = 120):
    tag_count_(tag_count), length_upper_bound_(length_upper_bound), log_manager_(), redis_utils_() {
    timestamp_delta_group_ = std::vector<std::vector<uint64_t>>(tag_count_);
    UpdateStartTimestamp();
}


void CacheManager::UpdateStartTimestamp() {
    std::time_t time_now = std::time(nullptr);
    uint64_t time_gap = 30 * 60;
    start_timestamp_ = time_now - time_gap;
}


bool CacheManager::WriteToCache(const std::string &data, std::string &timestamp, int tag_key) {
    // 
}