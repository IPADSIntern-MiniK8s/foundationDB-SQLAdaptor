#include "../cache/RedisUtils.h"
#include "TestFramework.h"
#include <spdlog/spdlog.h>

int main() {
    RedisUtils redis_utils;
    std::string value{"value"};
    std::string key{"key"};
    std::string int_key{"test_int"};
    ASSERT(redis_utils.RedisWrite(key, value) == true, "string insertion test fail");
    ASSERT(redis_utils.RedisWrite(int_key, 1) == true, "int insertion test fail");
    ASSERT(redis_utils.RedisRead("key") == "value", "read string test fail");
    ASSERT(redis_utils.RedisRead("test_int") == "1", "read int test fail");
    return 0;
}