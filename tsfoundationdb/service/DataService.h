#ifndef DATA_SERVICE_H_
#define DATA_SERVICE_H_

#include <utility>
#include <sstream>
#include <google/protobuf/text_format.h>
#include "spdlog/spdlog.h"
#include "../protodata/FieldMessage.pb.h"
#include "../protodata/RpcMessage.pb.h"

using Message::MessageEntry;
using FieldMessage::IntFieldList;
using FieldMessage::StrFieldList;

///@brief the tool class for pack and unpack protobuf data

class DataService {
public:
    static std::string SerializeMessage(MessageEntry *entry);
    
    static MessageEntry DeserializeMessage(const std::string &data);
    
    static std::pair<int, void*> SerializeIntFieldList(IntFieldList *list);
    
    // TODO: for implement
    // static IntFieldList DeserializeIntFieldList(const std::string &data);
    
    static std::pair<int, void*> SerializeStrFieldList(StrFieldList *list);
    
    // TODO: for implement
    // static StrFieldList DeserializeStrFieldList(const std::string &data);
};

#endif