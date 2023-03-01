#include "CacheManager.h"


CacheManager::CacheManager(int tag_count, int length_upper_bound):
    redis_utils_(), storage_tool_(nullptr), tag_range_(tag_count), length_upper_bound_(length_upper_bound), cur_length_(0) {
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
    // MetaDataManager::Setup("../test/input/demo.config");
    int n = MetaDataManager::GetAttributeCount();

    // clear the metadata information in the cache
    uint64_t save_timestamp = this->start_timestamp_;
    std::vector<std::vector<uint64_t>> save_groups = std::move(timestamp_delta_group_);

    // prepare the cache for coming data
    timestamp_delta_group_ = std::vector<std::vector<uint64_t>>(tag_range_);
    UpdateStartTimestamp();
    cur_length_ = 0;

    for (int i = 0; i < tag_range_; ++i) {
        threads.emplace_back(std::thread([this, n, i, save_timestamp, save_groups]() {
            uint64_t start_timestamp = save_timestamp;
            std::vector<uint64_t> timestamp_deltas = save_groups[i];

            // get the data type and generate buidler
            // FIXME: This step should be executed once by each thread, which may be repeated
            std::unordered_map<int, Field> attributes = MetaDataManager::GetAttributeList();
            
            // the corresponding relationship index and builder pos
            std::vector<int> relations(n + 2, 0);
            std::unordered_map<int, std::unique_ptr<FieldMessage::IntFieldList>> intfields;
            std::unordered_map<int, std::unique_ptr<FieldMessage::StrFieldList>> strfields;
            
            for (int j = 2; j <= n + 1; ++j) {
                if (attributes[j].kind == FIELD_KIND::OTHER || attributes[j].kind == FIELD_KIND::TAG) {
                    continue;
                }
                if (attributes[j].type == TYPE::INT) {
                    intfields[j] = std::move(std::make_unique<FieldMessage::IntFieldList>());
                } else if (attributes[j].type == TYPE::VARCHAR) {
                    strfields[j] = std::move(std::make_unique<FieldMessage::StrFieldList>());
                }
            }

            // read the data from the cache (redis)
            for (auto delta : timestamp_deltas) {
                std::string key = std::to_string(delta) + " " + std::to_string(i);
                std::string data = this->redis_utils_.RedisRead(key);
                if (!data.empty()) {
                    MessageEntry message = DataService::DeserializeMessage(data);
                    // TODO: may need some exception handler
                    
                    // compress each field into its block
                    const google::protobuf::Descriptor *des = message.GetDescriptor();
                    const google::protobuf::Reflection *ref = message.GetReflection();
                    int fieldCount = des->field_count();
                    for (int k = 1; k < fieldCount; ++k) {
                        const google::protobuf::FieldDescriptor *field = des->field(k);
                        switch (field->type()) {
                            case google::protobuf::FieldDescriptor::Type::TYPE_INT32:
                            case google::protobuf::FieldDescriptor::Type::TYPE_SINT32:
                            case google::protobuf::FieldDescriptor::Type::TYPE_SFIXED32:
                                {
                                    if (!intfields.count(k + 1)) {
                                        continue;
                                    }
                                    FieldMessage::IntField *new_field = intfields[k + 1]->add_fieldlist();
                                    int32_t data = ref->GetInt32(message, field);
                                    new_field->set_delta(delta);
                                    new_field->set_fieldvalue(data);
                                    break;
                                }
                            case google::protobuf::FieldDescriptor::Type::TYPE_UINT32:
                            case google::protobuf::FieldDescriptor::Type::TYPE_FIXED32:
                                {
                                    if (!intfields.count(k + 1)) {
                                        continue;
                                    }
                                    FieldMessage::IntField *new_field = intfields[k + 1]->add_fieldlist();
                                    int32_t data = ref->GetInt32(message, field);
                                    new_field->set_delta(delta);
                                    new_field->set_fieldvalue(data);
                                    break;
                                }
                            case google::protobuf::FieldDescriptor::Type::TYPE_STRING:
                            case google::protobuf::FieldDescriptor::Type::TYPE_BYTES:
                                {
                                    if (!strfields.count(k + 1)) {
                                        continue;
                                    }
                                    FieldMessage::StrField *new_field = strfields[k + 1]->add_fieldlist();
                                    std::string data = ref->GetString(message, field);
                                    new_field->set_delta(delta);
                                    new_field->set_fieldvalue(data);
                                    break;
                                }
                            default:
                                {
                                    spdlog::info("[CacheManager] unsupported type");
                                }
                        }    
                    }
                }
            }

            // generate key for each block
            // (measurement, tag set, one field, start timestamp, entry count)
            std::string measurement = MetaDataManager::GetMeasurement();
            // tag set = tag_name1 + tag_name2 + ... + tag_name3 + tag_key+value
            std::vector<Field> tags = MetaDataManager::GetTagList();
            std::string tag_set;
            for (auto &tag : tags) {      // TODO: now only support one tag
                tag_set += tag.name;
            }
            tag_set += std::to_string(i);
            std::string start_timestamp_str = std::to_string(start_timestamp);
            std::string entry_count_str = std::to_string(timestamp_deltas.size());
    
            for (auto &[int_key, elem] : intfields) {
                std::string field_name = attributes[int_key].name;
                std::string key = measurement + tag_set + field_name + start_timestamp_str + entry_count_str;
                auto value = DataService::SerializeIntFieldList(elem.get());
                if (value.first != 0) {
                    storage_tool_.tsSet(KeySelector(key), BinValue(value.second, value.first));
                }
            }

            for (auto &[str_key, elem] : strfields) {
                std::string field_name = attributes[str_key].name;
                std::string key = measurement + tag_set + field_name + start_timestamp_str + entry_count_str;
                auto value = DataService::SerializeStrFieldList(elem.get());
                if (value.first != 0) {
                    storage_tool_.tsSet(KeySelector(key), BinValue(value.second, value.first));
                }
            }
        }
        ));
    }


    // detach the thread
    for (auto &thread : threads) {
        thread.detach();
    }
}


bool CacheManager::ImmediateStore(const std::string &timestamp, int tag_key, const std::string &data) {
    std::string key = timestamp + std::to_string(tag_key);
    // TODO: maybe need change to asynchronous later
    storage_tool_.tsSet(KeySelector(key), BinValue(data));
    return true;
}



