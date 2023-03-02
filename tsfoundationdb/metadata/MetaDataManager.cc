#include "MetaDataManager.h"

std::string MetaDataManager::measurement_ = "";
int MetaDataManager::timestamp_pos = 1;
std::unordered_map<int, Field> MetaDataManager::attribute_list_ = std::unordered_map<int, Field>();
std::vector<Field> MetaDataManager::tag_list_ = std::vector<Field>();
std::vector<Field> MetaDataManager::field_list_ = std::vector<Field>();
std::vector<Field> MetaDataManager::other_list_ = std::vector<Field>();

TYPE MetaDataManager::JudgeType(const std::string &type) {
    if (type == "int" || type == "INT") {
        return TYPE::INT;
    } else if (type == "string" || type == "STRING" || type == "VARCHAR" || type == "varchar") {
        return TYPE::VARCHAR;
    } else if (type == "timestamp" || type == "TIMESTAMP") {
        return TYPE::TIMESTAMP;
    } else if (type == "text" || type == "TEXT" ) {
        return TYPE::TEXT;
    } else {
        spdlog::error("[MetaDataManager] the unknown tag type, the type: {}", type);
        return TYPE::UNKNOWN;
    }
}


MetaDataManager::MetaDataManager(const std::string &filepath) {
    if (attribute_list_.empty()) {
        Setup(filepath);
    }
}


void MetaDataManager::Setup(const std::string &filepath) {
    if (!measurement_.empty()) {
        return;
    }
    std::ifstream infile(filepath, std::ios::in);
    if (!infile) {
        spdlog::error("[MetaDataManager] metadata initialization fail");
        return;
    }

    std::string line;
    while(std::getline(infile, line)) {
        std::size_t pos = line.find('=');
        if (pos == std::string::npos) {
            spdlog::error("[MetaDataManager] read an illegal line");
        }

        std::string category = line.substr(0, pos);
        // get the measurement name
        if (category == "measurement") {
            MetaDataManager::measurement_ = line.substr(pos + 1);
        } else if (category == "tags" || category == "fields" || category == "others") {
            // assign the tags
            while (true) {
                spdlog::debug("[MetaDataManager] the line: {}, pos: {}", line, pos);
                std::size_t begin = line.find('(', pos);
                std::size_t end = line.find(')', pos);
                std::size_t mid = line.find(',', pos);
                
                if (begin == std::string::npos || end == std::string::npos || mid == std::string::npos) {
                    spdlog::debug("[MetaDataManager] the line end, the category: {}", category);
                    break;
                }
                std::size_t next_mid = line.find(',', mid + 1);
                if (next_mid == std::string::npos) {
                    spdlog::debug("[MetaDataManager] the line end because of next_mid, the category: {}", category);
                    break;
                }

                std::string name = line.substr(begin + 1, mid - begin - 1);
                TYPE type = JudgeType(line.substr(mid + 1, next_mid - mid - 1));
                int id = stoi(line.substr(next_mid + 1, end - next_mid - 1));
                spdlog::debug("[MetaDataManager] the id: {}, the type: {}, the name: {}", id, static_cast<int>(type), name);
                if (category == "tags") {
                    Field new_field(name, type, FIELD_KIND::TAG, id);
                    MetaDataManager::attribute_list_[id] = new_field;
                } else if (category == "fields") {
                    Field new_field(name, type, FIELD_KIND::FIELD, id);
                    MetaDataManager::attribute_list_[id] = new_field;
                } else {
                    Field new_field(name, type, FIELD_KIND::OTHER, id);
                    MetaDataManager::attribute_list_[id] = new_field;
                }

                spdlog::debug("[MetaDataManager] the attribute list size now: {}", MetaDataManager::attribute_list_.size());
                pos = end + 2;
            }
        }
    }
}


std::string MetaDataManager::GetMeasurement() {
    return MetaDataManager::measurement_;
}


std::vector<Field> MetaDataManager::GetTagList() {
    spdlog::debug("[MetaDataManager] the attribute list size now: {}", MetaDataManager::attribute_list_.size());
    if (!MetaDataManager::tag_list_.empty()) {
        spdlog::debug("[MetaDataManager] the tag list is not empty");
        return MetaDataManager::tag_list_;
    }
    for (auto [id, attr] : MetaDataManager::attribute_list_) {
        if (attr.kind == FIELD_KIND::TAG) {
            MetaDataManager::tag_list_.emplace_back(attr);
        }
    }
    return MetaDataManager::tag_list_;
}


std::vector<Field> MetaDataManager::GetFieldList() {
    if (!MetaDataManager::field_list_.empty()) {
        return field_list_;
    }
 
    for (auto [id, attr] : MetaDataManager::attribute_list_) {
        if (attr.kind == FIELD_KIND::FIELD) {
            MetaDataManager::field_list_.emplace_back(attr);
        }
    }
    return MetaDataManager::field_list_;
}


std::vector<Field> MetaDataManager::GetOtherList() {
    if (!MetaDataManager::other_list_.empty()) {
        return other_list_;
    }

    for (auto [id, attr] : attribute_list_) {
        if (attr.kind == FIELD_KIND::OTHER) {
            MetaDataManager::other_list_.emplace_back(attr);
        }
    }
    return MetaDataManager::other_list_;
}


std::unordered_map<int, Field> MetaDataManager::GetAttributeList() {
    return MetaDataManager::attribute_list_;
}


TYPE MetaDataManager::GetType(const std::string &name) {
    for (auto [id, attr] : MetaDataManager::attribute_list_) {
        if (attr.name == name) {
            return attr.type;
        }
    }
    return TYPE::UNKNOWN;
}



FIELD_KIND MetaDataManager::GetField(const std::string &name) {
    for (auto [id, attr] : MetaDataManager::attribute_list_) {
        if (attr.name == name) {
            return attr.kind;
        }
    }
    return FIELD_KIND::UNKNOWN;
}


int MetaDataManager::GetAttributeCount() {
    return MetaDataManager::attribute_list_.size();
}


FIELD_KIND MetaDataManager::GetFieldByIndex(int id) {
    if (!MetaDataManager::attribute_list_.count(id)) {
        return FIELD_KIND::UNKNOWN;
    }
    return MetaDataManager::attribute_list_[id].kind;
}


TYPE MetaDataManager::GetTypeByIndex(int id) {
    if (!MetaDataManager::attribute_list_.count(id)) {
        return TYPE::UNKNOWN;
    }
    return MetaDataManager::attribute_list_[id].type;
}