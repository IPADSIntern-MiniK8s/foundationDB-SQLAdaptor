#include "DataService.h"

std::string DataService::SerializeMessage(MessageEntry *entry) {
    std::string result;
    ///@note need to generate readable string
    
    // if (!entry->SerializeToString(&result)) {
    //     spdlog::error("[DataService] serialize message fail");
    // }
    google::protobuf::TextFormat::PrintToString(*entry, &result);
    // result = entry->DebugString();
    return result;
}


MessageEntry DataService::DeserializeMessage(const std::string &data) {
    MessageEntry message_entry;
    if (!message_entry.ParseFromString(data)) {
        spdlog::error("[DataService] serialize message fail");
    }
    return message_entry;
}


std::pair<int, void*> DataService::SerializeIntFieldList(IntFieldList *list) {
    int data_size = list->ByteSizeLong();
    void *data = malloc(data_size);
    if (!list->SerializeToArray(data, data_size)) {
        free(data);
        spdlog::error("[DataService] serialize intfieldlist fail");
        return std::pair<int, void*>(0, nullptr);
    } else {
        return std::pair<int, void*>(data_size, data);
    }
}


std::pair<int, void*> DataService::SerializeStrFieldList(StrFieldList *list) {
    int data_size = list->ByteSizeLong();
    void *data = malloc(data_size);
    if (!list->SerializeToArray(data, data_size)) {
        free(data);
        spdlog::error("[DataService] serialize strfieldlist fail");
        return std::pair<int, void*>(0, nullptr);
    } else {
        return std::pair<int, void*>(data_size, data);
    }
}