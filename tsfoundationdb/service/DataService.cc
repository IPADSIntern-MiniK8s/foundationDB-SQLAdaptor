#include "DataService.h"

std::string DataService::SerializeMessage(MessageEntry *entry) {
    std::string result;
    if (!entry->SerializeToString(&result)) {
        spdlog::error("[DataService] serialize message fail");
    }
    return result;
}


MessageEntry DataService::DeserializeMessage(const std::string &data) {
    MessageEntry message_entry;
    if (!message_entry.ParseFromString(data)) {
        spdlog::error("[DataService] serialize message fail");
    }
    return message_entry;
}