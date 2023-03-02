#include "../metadata/MetaDataManager.h"
#include "TestFramework.h"

int main() {
    spdlog::set_level(spdlog::level::debug);
    MetaDataManager::Setup("../test/input/demo.config");
    ASSERT(MetaDataManager::GetMeasurement() == "CAR_DATA", "the measurement name not correct, the current: " + MetaDataManager::GetMeasurement());
    std::vector<Field> tag_list = MetaDataManager::GetTagList();
    ASSERT(tag_list.size() == 1, "the tag set size is not correct, the current size: " + std::to_string(tag_list.size()));
    ASSERT(tag_list[0].kind == FIELD_KIND::TAG, "the tag kind is not correct, the current: " + std::to_string(static_cast<int>(tag_list[0].kind)));
    ASSERT(tag_list[0].name == "CAR_ID", "the tag name is not correct, the current: " + tag_list[0].name);

    std::vector<Field> field_list = MetaDataManager::GetFieldList();
    ASSERT(field_list.size() == 6, "the field set size is not correct, the current: " + std::to_string(field_list.size()));
    for (int i = 0; i < 6; ++i) {
        ASSERT(field_list[i].kind == FIELD_KIND::FIELD, "the field kind is not correct, the current: " + std::to_string(static_cast<int>(field_list[i].kind)));
    }
    // ASSERT(field_list[0].name == "X", "the field name is not correct, the current: " + field_list[0].name);

    std::vector<Field> other_list = MetaDataManager::GetOtherList();
    ASSERT(other_list.size() == 1, "the other set size is not correct, the current: " + std::to_string(other_list.size()));
    ASSERT(other_list[0].kind == FIELD_KIND::OTHER, "the tag kind is not correct, the current: " + std::to_string(static_cast<int>(other_list[0].kind)));
    ASSERT(other_list[0].name == "IMG", "the tag name is not correct, the current: " + other_list[0].name);

    ASSERT(MetaDataManager::GetType("X") == TYPE::INT, "the x's type not correct");
    ASSERT(MetaDataManager::GetType("IMG") == TYPE::TEXT, "the img's type not correct");

    ASSERT(MetaDataManager::GetField("Y") == FIELD_KIND::FIELD, "the y's kind is not correct");
    ASSERT(MetaDataManager::GetField("Z") == FIELD_KIND::UNKNOWN, "the z's kind should be illegal");

    ASSERT(MetaDataManager::GetAttributeCount() == 8, "the attribute number is not correct");

    ASSERT(MetaDataManager::GetFieldByIndex(2) == FIELD_KIND::TAG, "get field by index result is not correct");

    ASSERT(MetaDataManager::GetTypeByIndex(3) == TYPE::INT, "get type by index result is not correct");

    return 0;
}