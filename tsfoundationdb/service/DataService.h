#ifndef DATA_SERVICE_H_
#define DATA_SERVICE_H_

#include "spdlog/spdlog.h"
#include "../protodata/RpcMessage.pb.h"

using Message::MessageEntry;

///@brief the tool class for pack and unpack protobuf data

class DataService {
public:
    static std::string SerializeMessage(MessageEntry *entry);
    static MessageEntry DeserializeMessage(const std::string &data);
};

#endif