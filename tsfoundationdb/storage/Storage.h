#ifndef STORAGE_HPP
#define STORAGE_HPP
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <cstring>
#include <optional>
#include <random>
#include <tuple>
#include <vector>
// Works with FoundationDB C API
// https://foundationdb.com/documentation/c-api/

class KeySelector {
    friend KeySelector operator+(KeySelector selector, int offset) {
        selector.offset_ += offset;
        return selector;
    }
    friend KeySelector operator-(KeySelector selector, int offset) {
        selector.offset_ -= offset;
        return selector;
    }

  private:
    uint8_t *key_;
    size_t key_size_;
    int offset_;
    fdb_bool_t or_equal;

  public:
    KeySelector(const char *key, size_t key_size, fdb_bool_t or_equal, int offset)
        : key_size_(key_size), offset_(offset), or_equal(or_equal) {
        key_ = new uint8_t[key_size_];
        memcpy(key_, key, key_size_);
    }
    KeySelector(const uint8_t *key, size_t key_size, fdb_bool_t or_equal, int offset)
        : key_size_(key_size), offset_(offset), or_equal(or_equal) {
        key_ = new uint8_t[key_size_];
        memcpy(key_, key, key_size_);
    }
    KeySelector(const char *key, size_t key_size) : key_size_(key_size), offset_(1), or_equal(0) {
        key_ = new uint8_t[key_size_];
        memcpy(key_, key, key_size_);
    }

    KeySelector(const uint8_t *key, size_t key_size) : key_size_(key_size), offset_(1), or_equal(0) {
        key_ = new uint8_t[key_size_];
        memcpy(key_, key, key_size_);
    }

    KeySelector(const std::string &key) : key_size_(key.size()), offset_(0), or_equal(0) {
        key_ = new uint8_t[key_size_];
        memcpy(key_, key.c_str(), key_size_);
    }
    KeySelector(const KeySelector &other) {
        key_size_ = other.key_size_;
        offset_ = other.offset_;
        or_equal = other.or_equal;
        key_ = new uint8_t[key_size_];
        memcpy(key_, other.key_, key_size_);
    }
    ~KeySelector() { delete[] key_; }
    const uint8_t *key() const { return key_; }
    size_t key_size() const { return key_size_; }
    int offset() const { return offset_; }
    fdb_bool_t or_equal_flag() const { return or_equal; }
    static KeySelector last_less_than(const char *key, size_t key_size) {
        return KeySelector(FDB_KEYSEL_LAST_LESS_THAN(key, key_size));
    }
    static KeySelector last_less_or_equal(const char *key, size_t key_size) {
        return KeySelector(FDB_KEYSEL_LAST_LESS_OR_EQUAL(key, key_size));
    }
    static KeySelector first_greater_than(const char *key, size_t key_size) {
        return KeySelector(FDB_KEYSEL_FIRST_GREATER_THAN(key, key_size));
    }
    static KeySelector first_greater_or_equal(const char *key, size_t key_size) {
        return KeySelector(FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(key, key_size));
    }
    operator std::string() const {
        return std::string(reinterpret_cast<const char *>(key_), key_size_);
    }
};

class BinValue {
  private:
    uint8_t *value_;
    size_t value_size_;

  public:
    BinValue(const uint8_t *value, int value_size) : value_size_(value_size) {
        value_ = new uint8_t[value_size_];
        memcpy(value_, value, value_size_);
    }
    BinValue(const char *value, size_t value_size) : value_size_(value_size) {
        value_ = new uint8_t[value_size_];
        memcpy(value_, value, value_size_);
    }
    BinValue(const std::string &value) : value_size_(value.size()) {
        value_ = new uint8_t[value_size_];
        memcpy(value_, value.c_str(), value_size_);
    }
    BinValue(const BinValue &other) {
        value_size_ = other.value_size_;
        value_ = new uint8_t[value_size_];
        memcpy(value_, other.value_, value_size_);
    }
    ~BinValue() { delete[] value_; }
    const uint8_t *value() const { return value_; }
    size_t value_size() const { return value_size_; }
    operator std::string() const {
        return std::string(reinterpret_cast<const char *>(value_), value_size_);
    }
};

typedef std::vector<std::tuple<KeySelector, std::shared_ptr<BinValue>>> KeyValuePairs;
const static std::string TRANSACTION_ID_KEY = "transcationId";

class Storage {
  public:
    enum class TransactionError {
        SUCCESS = 0,
        NOT_COMMITTED = 1020,
        COMMIT_UNKNOWN_RESUTLT = 1021,
        TRANSACTION_CANCELLED = 1025,
    };
    Storage(const char *cluster_file);
    ~Storage();
    void begin_transaction();
    TransactionError commit_transaction();
    void abort_transaction();
    // Wrapper class for managing client-side transactions
    class Transaction {
      private:
        FDBTransaction *transaction_;
        Storage *storage_;
        static std::map<std::string, std::pair<FDBTransaction *, Storage *>> transaction_map;
        // todo: a big map will cause performance loss
        static std::mutex transaction_map_mutex;

        const std::string id_;
        Transaction(FDBTransaction *transaction, Storage *storage, std::string id)
            : transaction_(transaction), storage_(storage), id_(id) {
            transaction_map[id_] = std::make_pair(transaction_, storage_);
        }

      public:
        static Transaction new_transaction(const char *cluster_file);
        std::string id() const { return id_; }
        Storage *storage() const { return storage_; }
        static Transaction continue_from_id(const std::string &id);
        TransactionError commit();
        void abort();
    };

    std::pair<uint8_t*, size_t> tsGet(uint8_t *key, int key_size);

    void tsSet(uint8_t *key, int key_size, uint8_t *value, int value_size);

    // Get value from database
    // Return value is allocated by malloc()
    // Caller must free() the returned value
    // @param key Key to get
    std::optional<std::shared_ptr<BinValue>> get(const KeySelector &key, bool snapshot = false);
    // Set value to database
    // @param key Key to set
    // @param value Value to set
    void set(const KeySelector &key, const BinValue &value);
    // Delete value from database
    // @param key Key to delete
    void del(const KeySelector &key);
    // Get value from database within range
    // Reads all key-value pairs in the database snapshot represented by transaction
    // (potentially limited by limit, target_bytes, or mode) which have a key lexicographically
    // greater than or equal to the key resolved by the begin key selector and lexicographically
    // less than the key resolved by the end key selector.
    // @param begin_key Begin key to get
    // @param end_key End key to get
    // @param snapshot Snapshot flag, non-zero if this is a snapshot read.
    // @param limit Limit of values to get If non-zero, indicates the maximum number of key-value
    // pairs to return.
    // @param target_bytes If non-zero, indicates the maximum number of bytes to return.
    // @param iteration If mode is FDB_STREAMING_MODE_ITERATOR, this parameter should start at 1 and
    // be incremented by 1 for each successive call while reading this range. In all other cases it
    // is ignored.
    // @param reverse If non-zero, key-value pairs will be returned in reverse lexicographical order
    // beginning at the end of the range. Reading ranges in reverse is supported natively by the
    // database and should have minimal extra cost.
    // @param mode One of the FDBStreamingMode values indicating how the caller would like the data
    // in the range returned.
    // @return tuple of KeyValuePairs and more flag
    std::tuple<KeyValuePairs, bool> get_range(const KeySelector &begin_key,
                                              const KeySelector &end_key, bool snapshot,
                                              int limit = 0, int target_bytes = 0,
                                              int iteration = 1, bool reverse = false,
                                              FDBStreamingMode mode = FDB_STREAMING_MODE_ITERATOR);

    void atomic_add(const KeySelector &key, int64_t value);

    // -------------------- Async API --------------------
    // Async get value from database
    // Return value is allocated by malloc()
    // Caller must free() the returned value
    // @param key Key to get
    // @param snapshot Snapshot flag, non-zero if this is a snapshot read.
    std::future<std::optional<std::shared_ptr<BinValue>>> get_async(const KeySelector &key,
                                                                    bool snapshot = false);
    // Async get value from database within range
    // Reads all key-value pairs in the database snapshot represented by transaction
    // (potentially limited by limit, target_bytes, or mode) which have a key lexicographically
    // greater than or equal to the key resolved by the begin key selector and lexicographically
    // less than the key resolved by the end key selector.
    // @param begin_key Begin key to get
    // @param end_key End key to get
    // @param snapshot Snapshot flag, non-zero if this is a snapshot read.
    // @param limit Limit of values to get If non-zero, indicates the maximum number of key-value
    // pairs to return.
    // @param target_bytes If non-zero, indicates the maximum number of bytes to return.
    // @param iteration If mode is FDB_STREAMING_MODE_ITERATOR, this parameter should start at 1 and
    // be incremented by 1 for each successive call while reading this range. In all other cases it
    // is ignored.
    // @param reverse If non-zero, key-value pairs will be returned in reverse lexicographical order
    // beginning at the end of the range. Reading ranges in reverse is supported natively by the
    // database and should have minimal extra cost.
    // @param mode One of the FDBStreamingMode values indicating how the caller would like the data
    // in the range returned.
    // @return tuple of KeyValuePairs and more flag
    std::future<std::tuple<KeyValuePairs, bool>>
    get_range_async(const KeySelector &begin_key, const KeySelector &end_key, bool snapshot,
                    int limit = 0, int target_bytes = 0, int iteration = 1, bool reverse = false,
                    FDBStreamingMode mode = FDB_STREAMING_MODE_ITERATOR);
    // Setup function
    // Must be called before any other function once and only once for the whole program
    static void setup();
    // Teardown function
    // Must be called after all other functions once and only once for the whole program
    static void quit();

    static std::shared_ptr<Storage> create_shared(const char *cluster_file) {
        return std::make_shared<Storage>(cluster_file);
    }

  private:
    bool check_transaction();
    static bool init;
    FDB_database *db_;
    FDBTransaction *transaction_;
    static std::thread network_thread;
};
#endif // !STORAGE_HPP
