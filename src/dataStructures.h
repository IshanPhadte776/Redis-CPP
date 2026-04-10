#pragma once
#ifndef DATA_STRUCTURES_H
#define DATA_STRUCTURES_H

#include <string>
#include <vector>
#include <variant>  // <--- ADD THIS: Required for std::variant
#include <chrono>
// ==========================================
// 2. Data Structures & Global Store
// ==========================================

enum class KeyType {
  None, // Default for non-existent keys
  String, //Created via SET
  List,  // Created via RPUSH/LPUSH
  Set, // Created via SADD
  ZSet, // Created via ZADD
  Hash, // Created via HSET
  Stream, // Created via XADD
  VectorSet, // Created via VADD (Hypothetical Command for Vector Similarity Search)
};

// Map the enum to the string response required by Redis
inline std::string typeToString(KeyType t) {
    switch (t) {
        case KeyType::String:    return "string";
        case KeyType::List:      return "list";
        case KeyType::Set:       return "set";
        case KeyType::ZSet:      return "zset";
        case KeyType::Hash:      return "hash";
        case KeyType::Stream:    return "stream";
        case KeyType::VectorSet: return "vectorset";
        default:                 return "none";
    }
}


struct StreamID {
    long long ms = 0;
    long long seq = 0;

    static StreamID parse(const std::string& s) {
        size_t dash = s.find('-');
        if (dash == std::string::npos) return {0, 0};
        try {
            return {std::stoll(s.substr(0, dash)), std::stoll(s.substr(dash + 1))};
        } catch (...) { return {0, 0}; }
    }

    static StreamID parseRange(const std::string& s, bool is_start) {
        if (s == "-") return {0, 0};
        if (s == "+") return {LLONG_MAX, LLONG_MAX};

        size_t dash = s.find('-');
        if (dash == std::string::npos) {
            // No sequence provided
            return {std::stoll(s), is_start ? 0 : LLONG_MAX};
        }
        return {std::stoll(s.substr(0, dash)), std::stoll(s.substr(dash + 1))};
    }

    std::string toString() const {
        return std::to_string(ms) + "-" + std::to_string(seq);
    }

    // Monotonicity check: New ID must be strictly greater than last ID
    bool operator>(const StreamID& other) const {
        if (ms != other.ms) return ms > other.ms;
        return seq > other.seq;
    }

    // Comparison for binary search
    bool operator<(const StreamID& other) const {
        if (ms != other.ms) return ms < other.ms;
        return seq < other.seq;
    }
    bool operator<=(const StreamID& other) const {
        if (ms != other.ms) return ms < other.ms;
        return seq <= other.seq;
    }
};

struct StreamEntry {
  StreamID id;
  std::vector<std::pair<std::string, std::string>> fields;
};


using RedisValue = std::variant<
    std::string,                    // For SET
    std::vector<std::string>,       // For LIST (LPUSH/RPUSH)
    std::vector<StreamEntry>        // For STREAM (XADD)
>;


struct Node {
    RedisValue value;    // We definitely still need this for the actual values!
    KeyType type = KeyType::None;
    std::chrono::steady_clock::time_point expires_at;
    bool hasTTL = false;
};


struct ExpiryEntry {
    std::string key;
    std::chrono::steady_clock::time_point expires_at;

    friend bool operator>(const ExpiryEntry& a, const ExpiryEntry& b) {
        return a.expires_at > b.expires_at;
    }
};


#endif