#include "Storage.h"
#include <spdlog/spdlog.h>


std::thread Storage::network_thread;
std::map<std::string, std::pair<FDBTransaction *, Storage *>> Storage::Transaction::transaction_map;
std::mutex Storage::Transaction::transaction_map_mutex;
bool Storage::init = false;
Storage::Storage(const char *cluster_file) {
    if (!network_thread.joinable()) {
        setup();
    }
    spdlog::debug("FoundationDB network setup.");
    auto err = fdb_create_database(cluster_file, &db_);
    if (err) {
        spdlog::error("Storage init: Create database error. [errno={}]", err);
        return;
    }
    spdlog::debug("Storage init: Create database success.");
}
Storage::~Storage() { fdb_database_destroy(db_); }

void Storage::setup() {
    if (init == false) {
        init = true;
    } else {
        return;
    }
    static fdb_error_t err = fdb_select_api_version(FDB_API_VERSION);
    if (err) {
        spdlog::error("Storage init: Select api version error. [errno={}]", err);
        throw std::runtime_error("Select api version error.");
    }
    spdlog::debug("FoundationDB version={}", fdb_get_max_api_version());
    fdb_error_t net_err = fdb_setup_network();
    if (net_err) {
        spdlog::error("Storage init: Setup network error. [errno={}]", err);
        throw std::runtime_error("Setup network error.");
    }
    spdlog::debug("FoundationDB network setup.");
    network_thread = std::thread([]() {
        auto err = fdb_run_network();
        if (err) {
            spdlog::error("Storage init: Run network error. [errno={}]", err);
        }
        spdlog::debug("Network thread exit.");
    });
}

void Storage::quit() {
    auto err = fdb_stop_network();
    if (err) {
        spdlog::error("Storage quit: Stop network error. [errno={}]", err);
        throw std::runtime_error("Stop network error.");
    }
    spdlog::debug("FoundationDB network stop.");
    network_thread.join();
}

void Storage::begin_transaction() {
    fdb_error_t err = fdb_database_create_transaction(db_, &transaction_);
    if (err) {
        spdlog::error("Storage::begin_transaction: Create transaction error. [errno={}]", err);
        throw std::runtime_error("Create transaction error.");
    }
}

bool Storage::check_transaction() {
    if (transaction_ == nullptr) {
        return false;
    }
    return true;
}



std::pair<uint8_t*, size_t> Storage::tsGet(uint8_t *key, int key_size) {
    begin_transaction();
    auto result = get(KeySelector(key, key_size));
    abort_transaction();
    std::size_t size = result->get()->value_size();
    uint8_t *data = new uint8_t[size];
    memcpy(data, result->get()->value(), size);
    return std::pair<uint8_t*, size_t>(data, result->get()->value_size());
}


void Storage::tsSet(uint8_t *key, int key_size, uint8_t *value, int value_size) {
    begin_transaction();
    this->set(KeySelector(key, key_size), BinValue(value, value_size));
    commit_transaction();
}



Storage::TransactionError Storage::commit_transaction() {
    auto future = fdb_transaction_commit(transaction_);
    auto ret = TransactionError::SUCCESS;
    if ((fdb_future_block_until_ready(future)) != 0) {
        spdlog::error("Storage::commit_transaction: Commit transaction error. errno={}", fdb_future_get_error(future));
        fdb_future_destroy(future);
        throw std::runtime_error("Commit transaction error.");
    }
    auto error = fdb_future_get_error(future);
    if (error) {
        spdlog::error("Storage::Transaction::commit: error errno={}", error);
        
        future = fdb_transaction_on_error(transaction_, error);
        if ((fdb_future_block_until_ready(future)) != 0) {
            error = fdb_future_get_error(future);
            spdlog::error("Storage::commit_transaction: Commit transaction error. errno={}", error);
            fdb_future_destroy(future);
            return TransactionError(error);
        }
        ret = TransactionError(error);
    }

    fdb_future_destroy(future);
    return ret;
}

void Storage::abort_transaction() {
    if (check_transaction()) {
        fdb_transaction_cancel(transaction_);
        spdlog::debug("Storage::abort_transaction: Abort transaction success.");
        return;
    }
    throw std::runtime_error("Transaction not created.");
}

void Storage::set(const KeySelector &key, const BinValue &value) {
    if (!check_transaction()) {
        throw std::runtime_error("Transaction not created.");
    }
    fdb_transaction_set(transaction_, key.key(), key.key_size(), value.value(), value.value_size());
    spdlog::debug("Storage::set: Set value success. [key={}]", key.key());
}

void Storage::del(const KeySelector &key) {
    if (!check_transaction()) {
        throw std::runtime_error("Transaction not created.");
    }
    fdb_transaction_clear(transaction_, key.key(), key.key_size());
    spdlog::debug("Storage::del: Delete value success.");
}

std::optional<std::shared_ptr<BinValue>> Storage::get(const KeySelector &key,
                                                      bool snapshot /*  = false */) {
    if (!check_transaction()) {
        throw std::runtime_error("Transaction not created.");
    }

    auto future = fdb_transaction_get(transaction_, key.key(), key.key_size(), snapshot);
    if ((fdb_future_block_until_ready(future)) != 0) {
        spdlog::error("Storage::get: Get value error. errno= {}", fdb_future_get_error(future));
        throw std::runtime_error("Get value error.");
    }
    fdb_bool_t is_present;
    const uint8_t *value;
    int value_size;
    if (fdb_future_get_value(future, &is_present, &value, &value_size) != 0) {
        spdlog::error("Storage::get: Get value error. errno= {}", fdb_future_get_error(future));
        throw std::runtime_error("Get value error.");
    }
   
    if (is_present) {
        auto ret = std::make_shared<BinValue>(value, value_size);
        spdlog::debug("Value found. key = {}", key.key());
        fdb_future_destroy(future);
        return std::make_optional(ret);
    }
    spdlog::debug("Value not found. [key={}]", key.key());
    fdb_future_destroy(future);
    return std::nullopt;
}

std::tuple<KeyValuePairs, bool> Storage::get_range(const KeySelector &begin_key,
                                                   const KeySelector &end_key, bool snapshot,
                                                   int limit, int target_bytes, int iteration,
                                                   bool reverse, FDBStreamingMode mode) {
    if (!check_transaction()) {
        throw std::runtime_error("Transaction not created.");
    }
    auto future = fdb_transaction_get_range(
        transaction_, begin_key.key(), begin_key.key_size(), begin_key.or_equal_flag(),
        begin_key.offset(), end_key.key(), end_key.key_size(), end_key.or_equal_flag(),
        end_key.offset(), limit, target_bytes, mode, iteration, snapshot, reverse);
    if ((fdb_future_block_until_ready(future)) != 0) {
        spdlog::error("Storage::get_range: Get range error. [errno={}]",
                      fdb_future_get_error(future));
        throw std::runtime_error("Get range error.");
    }
    fdb_bool_t more;
    KeyValuePairs kv_pairs;
    int value_count;
    const FDBKeyValue *kv;
    if (fdb_future_get_keyvalue_array(future, &kv, &value_count, &more) != 0) {
        spdlog::error(
            "Storage::get_range: Get range error when get keyvalue from future. [errno={}]",
            fdb_future_get_error(future));
        throw std::runtime_error("Get range error.");
    }
    if (more) {
        spdlog::debug("Storage::get_range: Get range success.");
    } else {
        spdlog::debug("Storage::get_range: Get range success. [more={}]", more);
    }
    for (int i = 0; i < value_count; i++) {
        kv_pairs.emplace_back(
            std::make_tuple(KeySelector(static_cast<uint8_t*>(const_cast<void*>(kv[i].key)), (size_t)kv[i].key_length, (fdb_bool_t)0, 0),
                            std::make_shared<BinValue>(BinValue(static_cast<uint8_t*>(const_cast<void*>(kv[i].value)), kv[i].value_length))));
    }
    fdb_future_destroy(future);
    return std::make_tuple(kv_pairs, more);
}
void Storage::atomic_add(const KeySelector &key, int64_t value) {
    if (!check_transaction()) {
        throw std::runtime_error("Transaction not created.");
    }
    fdb_transaction_atomic_op(transaction_, key.key(), key.key_size(),
                              reinterpret_cast<const uint8_t *>(&value), sizeof(value),
                              FDB_MUTATION_TYPE_ADD);
}

std::future<std::optional<std::shared_ptr<BinValue>>> Storage::get_async(const KeySelector &key,
                                                                         bool snapshot) {
    if (!check_transaction()) {
        throw std::runtime_error("Transaction not created.");
    }
    auto future = fdb_transaction_get(transaction_, key.key(), key.key_size(), snapshot);
    return std::async([future, key]() -> std::optional<std::shared_ptr<BinValue>> {
        if ((fdb_future_block_until_ready(future)) != 0) {
            spdlog::error("Storage::get_async: Get value error. [errno={}]",
                          fdb_future_get_error(future));
            throw std::runtime_error("Get value error when waiting for the future.");
        }
        fdb_bool_t is_present;
        const uint8_t *value;
        int value_size;
        if (fdb_future_get_value(future, &is_present, &value, &value_size) != 0) {
            spdlog::error("Storage::get_async: Get value error. [errno={}]",
                          fdb_future_get_error(future));
            throw std::runtime_error("Get value error.");
        }
        if (is_present) {
            spdlog::debug("Storage::get_async: Get value success. [key={}]", key.key());
            auto ret = std::make_shared<BinValue>(value, value_size);
            fdb_future_destroy(future);
            return std::make_optional(ret);
        }
        fdb_future_destroy(future);
        spdlog::debug("Value not found. [key={}]", key.key());
        return std::nullopt;
    });
}

std::future<std::tuple<KeyValuePairs, bool>>
Storage::get_range_async(const KeySelector &begin_key, const KeySelector &end_key, bool snapshot,
                         int limit, int target_bytes, int iteration, bool reverse,
                         FDBStreamingMode mode) {
    if (!check_transaction()) {
        throw std::runtime_error("Transaction not created.");
    }
    auto future = fdb_transaction_get_range(
        transaction_, begin_key.key(), begin_key.key_size(), begin_key.or_equal_flag(),
        begin_key.offset(), end_key.key(), end_key.key_size(), end_key.or_equal_flag(),
        end_key.offset(), limit, target_bytes, mode, iteration, snapshot, reverse);
    return std::async([future]() -> std::tuple<KeyValuePairs, bool> {
        if ((fdb_future_block_until_ready(future)) != 0) {
            spdlog::error("Get range error. [errno={}]", fdb_future_get_error(future));
            throw std::runtime_error("Get range error.");
        }
        fdb_bool_t more;
        KeyValuePairs kv_pairs;
        int value_count;
        const FDBKeyValue *kv;
        if (fdb_future_get_keyvalue_array(future, &kv, &value_count, &more) != 0) {
            spdlog::error("Get range error when get keyvalue from future. [errno={}]",
                          fdb_future_get_error(future));
            throw std::runtime_error("Get range error.");
        }
        if (more) {
            spdlog::debug("Get range success.");
        } else {
            spdlog::debug("Get range success. [more={}]", more);
        }
        for (int i = 0; i < value_count; i++) {
            kv_pairs.emplace_back(std::make_tuple(
                KeySelector(static_cast<uint8_t*>(const_cast<void*>(kv[i].key)), kv[i].key_length, 0, 0),
                std::make_shared<BinValue>(BinValue(static_cast<uint8_t*>(const_cast<void*>(kv[i].value)), kv[i].value_length))));
        }
        fdb_future_destroy(future);
        return std::make_tuple(kv_pairs, more);
    });
}


std::string random_string(std::size_t length)
{
    const std::string CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::uniform_int_distribution<> distribution(0, CHARACTERS.size() - 1);

    std::string random_string;

    for (std::size_t i = 0; i < length; ++i)
    {
        random_string += CHARACTERS[distribution(generator)];
    }

    return random_string;
}

Storage::Transaction Storage::Transaction::new_transaction(const char *cluster_file) {
    FDBTransaction *transaction;
    auto storage = new Storage(cluster_file);
    fdb_error_t error = fdb_database_create_transaction(storage->db_, &transaction);
    if (error) {
        throw std::runtime_error("Failed to begin transaction");
    }
    storage->transaction_ = transaction;
    std::string id = random_string(10);


    std::lock_guard<std::mutex> lock(Storage::Transaction::transaction_map_mutex);
    Storage::Transaction::transaction_map[id] = std::make_pair(transaction, storage);

    spdlog::debug("Storage::Transaction::new_transaction: Begin transaction success. [id={}]", id);
    return Transaction(transaction, storage, id);
}
Storage::Transaction Storage::Transaction::continue_from_id(const std::string &id) {
    FDBTransaction *transaction;
    std::lock_guard<std::mutex> lock(Storage::Transaction::transaction_map_mutex);
    auto it = transaction_map.find(id);
    if (it == transaction_map.end()) {
        throw std::runtime_error("Failed to continue transaction");
    }
    transaction = it->second.first;
    auto storage = it->second.second;
    spdlog::debug("Storage::Transaction::continue_from_id: Continue transaction success. [id={}]",
                  id);
    return Transaction(transaction, storage, id);
}
Storage::TransactionError Storage::Transaction::commit() {
    auto ret = storage_->commit_transaction();
    std::lock_guard<std::mutex> lock(Storage::Transaction::transaction_map_mutex);
    Storage::Transaction::transaction_map.erase(id_);
    delete storage_;
    return ret;
}
void Storage::Transaction::abort() {
    fdb_transaction_cancel(transaction_);
    std::lock_guard<std::mutex> lock(Storage::Transaction::transaction_map_mutex);
    Storage::Transaction::transaction_map.erase(id_);
    delete storage_;
    spdlog::debug("Storage::Transaction::abort: Abort transaction success.");
}

