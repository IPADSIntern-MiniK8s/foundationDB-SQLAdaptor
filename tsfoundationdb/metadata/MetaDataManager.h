#ifndef METADATA_MANAGER_H_
#define METADATA_MANAGER_H_
#include <string>
#include <fstream>
#include <vector>
#include "spdlog/spdlog.h"

enum class TYPE {
    INT,
    VARCHAR,
    TIMESTAMP,
    UNKNOWN,
};

struct Tag {
    std::string name;
    TYPE type;
    
    Tag() {}
    Tag(const std::string &new_name, TYPE new_type): name(new_name), type(new_type) {}
};


struct Field
{
    std::string name;
    TYPE type;

    Field() {}
    Field(const std::string &new_name, TYPE new_type): name(new_name), type(new_type) {}
};


class MetaDataManager {
private:
    static std::string measurement_;
    static std::vector<Tag> tag_list_;
    static std::vector<Field> field_list_;

    static TYPE JudgeType(const std::string &type);

public:
    ///@brief help load the config file
    MetaDataManager(const std::string &filepath);

    static std::vector<Tag> GetTagList();

    static void Setup(const std::string &filepath);

    static std::vector<Field> GetFieldList();

    ///@brief get the tag's type, for illegal type, return unknown
    static  TYPE GetTagType(const std::string &tag_name);

    static TYPE GetFieldType(const std::string &field_name);
};

#endif