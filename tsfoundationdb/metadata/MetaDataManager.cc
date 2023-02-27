#include "MetaDataManager.h"

std::string MetaDataManager::measurement_ = "";


TYPE MetaDataManager::JudgeType(const std::string &type) {
    if (type == "int" || type == "INT") {
        return TYPE::INT;
    } else if (type == "string" || type == "TYPE" || type == "VARCHAR" || type == "varchar") {
        return TYPE::VARCHAR;
    } else if (type == "timestamp" || type == "TIMESTAMP") {
        return TYPE::TIMESTAMP;
    } else {
        spdlog::error("[MetaDataManager] the unknown tag type");
        return TYPE::UNKNOWN;
    }
}


MetaDataManager::MetaDataManager(const std::string &filepath) {
    if (tag_list_.empty()) {
        Setup(filepath);
    }
}


void MetaDataManager::Setup(const std::string &filepath) {
    std::ifstream infile(filepath, std::ios::in);
    if (!infile) {
        spdlog::error("[MetaDataManager] metadata initialization fail");
        return;
    }

    std::string line;
    while(std::getline(infile, line)) {
        int pos = line.find('=');
        if (pos == std::string::npos) {
            spdlog::error("[MetaDataManager] read an illegal line");
        }

        std::string category = line.substr(0, pos);
        // get the measurement name
        if (category == "measurement") {
            measurement_ = line.substr(pos + 1);
        } else if (category == "tags" || category == "fields") {
            // assign the tags
            while (true) {
                int begin = line.find(pos, '(');
                int end = line.find(pos, ')');
                int mid = line.find(pos, ',');
                if (begin == std::string::npos || end == std::string::npos || mid == std::string::npos) {
                    break;
                }

                std::string name = line.substr(begin, mid - begin);
                TYPE type = JudgeType(line.substr(mid + 1, end - mid - 1));
                if (category == "tags") {
                    Tag new_tag(name, type);
                    tag_list_.emplace_back(new_tag);
                } else {
                    Field new_field(name, type);
                    field_list_.emplace_back(new_field);
                }
                
                pos = end + 1;
            }
        } 
    }
}


std::vector<Tag> MetaDataManager::GetTagList() {
    return tag_list_;
}


std::vector<Field> MetaDataManager::GetFieldList() {
    return field_list_;
}


TYPE MetaDataManager::GetTagType(const std::string &tag_name) {
    for (const auto &tag : tag_list_) {
        if (tag.name == tag_name) {
            return tag.type;
        }
    }
    return TYPE::UNKNOWN;
}


TYPE MetaDataManager::GetFieldType(const std::string &field_name) {
    for (const auto &field : field_list_) {
        if (field.name == field_name) {
            return field.type;
        }
    }
    return TYPE::UNKNOWN;
}
