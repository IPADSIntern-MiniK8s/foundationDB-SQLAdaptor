#include "DataService.h"

std::string DataService::SerializeMessage(MessageEntry *entry) {
    std::string result;
    ///@note need to generate readable string
    
    if (!entry->SerializeToString(&result)) {
        spdlog::error("[DataService] serialize message fail");
    }
    // google::protobuf::TextFormat::PrintToString(*entry, &result);
    // const google::protobuf::Descriptor *des = entry->GetDescriptor();
    // const google::protobuf::Reflection *ref = entry->GetReflection();
    // int fieldCount = des->field_count();
    // for (int i = 0; i < fieldCount; ++i) {
    //     const google::protobuf::FieldDescriptor *field = des->field(i);
    //     switch (field->type()) {
    //         case google::protobuf::FieldDescriptor::Type::TYPE_INT32:
    //         case google::protobuf::FieldDescriptor::Type::TYPE_SINT32:
    //         case google::protobuf::FieldDescriptor::Type::TYPE_SFIXED32:
    //             result += (std::to_string(ref->GetInt32(*entry, field)) + "$");
    //             break;
    //         case google::protobuf::FieldDescriptor::Type::TYPE_UINT32:
    //         case google::protobuf::FieldDescriptor::Type::TYPE_FIXED32:
    //             result += (std::to_string(ref->GetUInt32(*entry, field)) + "$");
    //             break;
    //         case google::protobuf::FieldDescriptor::Type::TYPE_STRING:
    //         case google::protobuf::FieldDescriptor::Type::TYPE_BYTES:
    //             result += (ref->GetString(*entry, field) + "$");
    //             break;
    //         default:
    //             spdlog::error("[DataService] unsupport message type");
    //             break;
    //     }
    // }
    // result = entry->DebugString();
    return result;
}


MessageEntry* DataService::DeserializeMessage(const std::string &data) {
    MessageEntry *message_entry = new MessageEntry();
    // const google::protobuf::Descriptor *des = message_entry->GetDescriptor();
    // const google::protobuf::Reflection *ref = message_entry->GetReflection();
    // int fieldCount = des->field_count();
    // spdlog::debug("the field count: {}", fieldCount);
    if (!message_entry->ParseFromString(data)) {
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