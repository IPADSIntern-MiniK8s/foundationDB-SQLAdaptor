#include "UploadService.h"

UploadService::UploadService(int tag_count = 4, int length_upper_bound = 120):
    cache_manager_(tag_count, length_upper_bound), tag_count_(tag_count), log_manager_() {
        MetaDataManager::Setup("../test/input/demo.config");
    }


bool UploadService::UploadData(MessageEntry *entry) {
    const google::protobuf::Descriptor *des = entry->GetDescriptor();
    const google::protobuf::Reflection *ref = entry->GetReflection();
    int fieldCount = des->field_count();
    
    // default: the first field is timestamp
    auto *time_field = des->field(0);
    if (time_field->type() != google::protobuf::FieldDescriptor::Type::TYPE_STRING) {
        spdlog::error("[UploadService] the timestamp should be string type");
        return false;
    }
    std::string timestamp = ref->GetString(*entry, time_field);
    std::vector<int> others_index_list;

    // the key for decide the tag set
    int hash_key = 0;

    // generate the tag key
    for (int i = 2; i <= fieldCount; ++i) {
        FIELD_KIND kind = MetaDataManager::GetFieldByIndex(i);
        if (kind == FIELD_KIND::UNKNOWN) {
            spdlog::error("[UploadService] the format of the input content does not match that in the config file");
            return false;
        } else if (kind == FIELD_KIND::OTHER) {
            others_index_list.emplace_back(i);
            continue;
        } else if (kind == FIELD_KIND::TAG) {
            TYPE type = MetaDataManager::GetTypeByIndex(i);
            
            if (type == TYPE::UNKNOWN) {
                spdlog::error("[UploadService] the tag {} type is unknown", i);
                continue;
            } else if (type == TYPE::INT) {
                hash_key = (hash_key + ref->GetUInt32(*entry, des->field(i - 1))) % tag_count_;
            } else if (type == TYPE::TEXT || type == TYPE::VARCHAR) {
                hash_key = (hash_key + std::hash<std::string>{}(ref->GetString(*entry, des->field(i - 1)))) % tag_count_;
            }
        }
    }

    // store into cache first
    std::string raw_data = DataService::SerializeMessage(entry);
    log_manager_.WriteToLog(raw_data);

    // first store the `other` data
    for (const auto &id : others_index_list) {
        TYPE type = MetaDataManager::GetTypeByIndex(id);
        if (type == TYPE::INT) {
            cache_manager_.ImmediateStore(timestamp, hash_key, std::to_string(ref->GetUInt32(*entry, des->field(id - 1))));
            // clear this field in entry
            ref->SetString(entry, des->field(id - 1), "");
        }
    } 

    // write into cache 
    std::string data = DataService::SerializeMessage(entry);
    cache_manager_.WriteToCache(data, timestamp, hash_key);
    
    // judge wether need to trigger flush
    if (cache_manager_.TriggerFlush()) {
        cache_manager_.FlushCache();
    }
    return true;
}