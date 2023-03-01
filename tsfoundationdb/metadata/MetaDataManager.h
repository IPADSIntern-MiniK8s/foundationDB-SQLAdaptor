#ifndef METADATA_MANAGER_H_
#define METADATA_MANAGER_H_
#include <string>
#include <fstream>
#include <vector>
#include <unordered_map>
#include "spdlog/spdlog.h"

enum class TYPE {
    INT,
    VARCHAR,
    TEXT,
    TIMESTAMP,
    UNKNOWN,
};

enum class FIELD_KIND {
    TAG,
    FIELD,
    OTHER,
    UNKNOWN
};

struct Field
{
    std::string name;
    TYPE type;
    FIELD_KIND kind;
    int pos;

    Field() {}
    Field(const std::string &new_name, TYPE new_type, FIELD_KIND new_kind, int new_pos): 
        name(new_name), type(new_type), kind(new_kind), pos(new_pos) {}
};

///@note timestamp defaults to the first position
class MetaDataManager {
private:
    static std::string measurement_;
    static std::unordered_map<int, Field> attribute_list_;
    static int timestamp_pos;
    static std::vector<Field> tag_list_;
    static std::vector<Field> field_list_;
    static std::vector<Field> other_list_;
    static TYPE JudgeType(const std::string &type);

public:
    ///@brief help load the config file
    MetaDataManager(const std::string &filepath);

    static std::vector<Field> GetTagList();

    static std::string GetMeasurement();

    static void Setup(const std::string &filepath);

    static std::vector<Field> GetFieldList();

    static std::vector<Field> GetOtherList();

    static std::unordered_map<int, Field> GetAttributeList();

    ///@brief get the tag's type, for illegal type, return unknown
    static TYPE GetType(const std::string &name);

    static FIELD_KIND GetField(const std::string &name);

    static int GetAttributeCount();

    static FIELD_KIND GetFieldByIndex(int id);

    static TYPE GetTypeByIndex(int id);

};

#endif