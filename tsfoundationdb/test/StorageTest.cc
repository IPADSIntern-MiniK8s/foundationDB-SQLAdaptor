#include <iostream>
#include <cstring>
#include "../storage/Storage.h"
#include "TestFramework.h"

int main() {
    uint8_t *key = new uint8_t[3];
    uint8_t *value = new uint8_t[5];
    auto key_str = "key";
    auto val_str = "value";
    memcpy(key, key_str, strlen(key_str));
    memcpy(value, val_str, strlen(val_str));
    Storage storage(nullptr);
    storage.tsSet(key, 3, value, 5);
    auto result = storage.tsGet(key, 3);
    ASSERT(result.second == 5, "test_set_get_value failed");
    ASSERT(memcmp(result.first, value, 5) == 0, "test_set_get_value failed");
    Storage::quit();
    return 0;
}