#include "CacheManager.h"


CacheManager::CacheManager(int tag_count, int length_upper_bound):
    tag_range_(tag_count), length_upper_bound_(length_upper_bound), redis_utils_(), cur_length_(0) {
    timestamp_delta_group_ = std::vector<std::vector<uint64_t>>(tag_range_);
    UpdateStartTimestamp();
}


void CacheManager::UpdateStartTimestamp() {
    std::time_t time_now = std::time(nullptr);
    uint64_t time_gap = 30 * 60;
    start_timestamp_ = time_now - time_gap;
}


bool CacheManager::WriteToCache(const std::string &data, std::string &timestamp, int tag_key) {
    // judge the timestamp
    uint64_t raw_timestamp = 0;
    try {
        raw_timestamp = std::stoul(timestamp);
    } catch (std::invalid_argument) {
        spdlog::error("[CacheManager] timestamp is illegal");
        return false;
    }

    // the message is expired
    if (raw_timestamp < start_timestamp_) {
        spdlog::info("[CacheManager] the message is expired");
        return false;
    }

    // check tag_key
    if (tag_key < 0 || tag_key >= tag_range_) {
        spdlog::info("[CacheManager] tag key is out of range");
        return false;
    }

    // insert to log
    uint64_t time_delta = raw_timestamp - start_timestamp_;
    timestamp_delta_group_[tag_key].emplace_back(time_delta);

    // update size record
    cur_length_ == std::max(cur_length_, (int) timestamp_delta_group_[tag_key].size());
    
    // asynchronous write cache
    std::thread cache_insert_thread([data, time_delta, tag_key, this]() {
        std::string cache_key = std::to_string(time_delta) + "_" + std::to_string(tag_key);
        this->redis_utils_.RedisWrite(cache_key, data);
    });
    cache_insert_thread.detach();
    return true;
}


bool CacheManager::TriggerFlush() {
    return cur_length_ >= length_upper_bound_;
}   


void CacheManager::FlushCache() {
    // one thread for each tag set
    std::vector<std::thread> threads;
    MetaDataManager::Setup("../test/input/demo.config");
    int n = MetaDataManager::GetAttributeCount();

    for (int i = 0; i < tag_range_; ++i) {
        threads.emplace_back(std::thread([this, n, i]() {
            uint64_t start_timestamp = this->start_timestamp_;
            
            std::vector<uint64_t> timestamp_deltas = this->timestamp_delta_group_[i];

            // get the data type and generate buidler
            // FIXME: This step should be executed once by each thread, which may be repeated
            std::unordered_map<int, Field> attributes = MetaDataManager::GetAttributeList();
            
            // the corresponding relationship index and builder pos
            std::vector<int> relations(n + 2, 0);
            std::unordered_map<int, std::unique_ptr<FieldMessage::IntFieldList>> intfields;
            std::unordered_map<int, std::unique_ptr<FieldMessage::StrFieldList>> strfileds;
            
            for (int j = 2; j <= n + 1; ++j) {
                if (attributes[j].type == TYPE::INT) {
                    intfields[j] = std::move(std::make_unique<FieldMessage::IntFieldList>());
                } else if (attributes[j].type == TYPE::VARCHAR) {
                    strfileds[j] = std::move(std::make_unique<FieldMessage::StrFieldList>());
                }
            }

            // read the data from the cache (redis)
            for (auto delta : timestamp_deltas) {
                std::string key = std::to_string(delta) + " " + std::to_string(i);
                std::string data = this->redis_utils_.RedisRead(key);
                if (!data.empty()) {
                    MessageEntry this_entry = DataService::DeserializeMessage(data);
                    // TODO: may need some error handler
                    
                }
            }
        }
        ));
    }
}