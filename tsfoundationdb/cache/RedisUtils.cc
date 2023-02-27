#include "RedisUtils.h"

redisContext* RedisUtils::context_ = nullptr;


void RedisUtils::RedisConnect() {
    context_ = redisConnect("127.0.0.1", 6379);
    if (context_ == NULL || context_->err) {
        if (context_) {
            spdlog::error("[RedisUtils] redis connect error: {}",  context_->errstr);
            // handle error
        } else {
            spdlog::error("[RedisUtils] can't allocate redis context");
        }
    }
}


RedisUtils::RedisUtils() {
    if (context_ == nullptr) {
        RedisConnect();
    }
}


RedisUtils::~RedisUtils() {
    redisFree(context_);
    context_ = nullptr;
}


bool RedisUtils::RedisWrite(std::string &key, uint32_t value) {
    std::string command = "SET " + key + " %u";
    auto reply = static_cast<redisReply*>(redisCommand(context_, command.c_str(), value));
    if (reply == nullptr) { 
        spdlog::error("[RedisUtils] execute redis write int fail (nullptr) ");
        return false; 
    } 
    if (!(reply->type == REDIS_REPLY_STATUS && strcasecmp(reply->str,"OK")==0)) { 
        spdlog::error("[RedisUtils] execute redis write int fail (error state) ");
        freeReplyObject(reply); 
        return false; 
    }    
    freeReplyObject(reply); 
    return true;
}


bool RedisUtils::RedisWrite(std::string &key, const std::string &value) {
    std::string command = "SET " + key + " " + value;
    auto reply = static_cast<redisReply*>(redisCommand(context_, command.data()));
    if (reply == nullptr) { 
        spdlog::error("[RedisUtils] execute redis write string fail (nullptr)");
        return false; 
    } 
    if (!(reply->type == REDIS_REPLY_STATUS && strcasecmp(reply->str,"OK")==0)) { 
        spdlog::error("[RedisUtils] execute redis write string fail (error state)");
        freeReplyObject(reply); 
        return false; 
    }    
    freeReplyObject(reply); 
    return true;
}


std::string RedisUtils::RedisRead(const std::string &key) {
    std::string command = "GET " + key;
    auto reply = static_cast<redisReply*>(redisCommand(context_, command.c_str()));
    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) { 
        spdlog::error("[RedisUtils] execute redis read fail, the illegal state: {}", reply->type);
        freeReplyObject(reply); 
        return ""; 
    } 

    if (reply->type == REDIS_REPLY_NIL) {
        spdlog::info("[RedisUtils] no such element with key {}", key);
        freeReplyObject(reply); 
        return "";
    }

    if (reply->type == REDIS_REPLY_STRING) {
        ///@note remember to free the reply object
        std::string result = reply->str;
        freeReplyObject(reply);
        return result;
    }

    if (reply->type == REDIS_REPLY_INTEGER) {
        spdlog::debug("get line 88");
        std::string result = std::to_string(reply->integer);
        ///@note remember to free the reply object
        freeReplyObject(reply); 
        return std::to_string(reply->integer);
    } 

    
    
    freeReplyObject(reply);
    return "";
}