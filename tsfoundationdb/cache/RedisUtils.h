#ifndef REDIS_UTILS_H_
#define REDIS_UTILS_H_

#include <string>
#include <hiredis/hiredis.h> 
#include "spdlog/spdlog.h"

///@brief help manage the operation about redis
///@note the redisContext is not thread safe
class RedisUtils {
private:
    static redisContext* context_;

public:
    RedisUtils();

    ~RedisUtils();

    static void RedisConnect();

    bool RedisWrite(std::string &key, uint32_t value);

    bool RedisWrite(std::string &key, std::string &value);
    
    std::string RedisRead(const std::string &key);
};
#endif
