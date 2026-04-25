//Code for the RESP parser implementation

//Understanding how RESP works 
// When the clients sends "PING", client sends the following RESP message: "*1\r\n$4\r\nPING\r\n"
// Example 1. RESP message consists of:
// *1 = Array of 1 element (PING is an array of 1 element)
// $4 = Bulk string of length 4 (PING is a bulk string of length)
// PING = The actual command being sent by the client
// \r\n = Carriage return and newline, used to indicate the end of a RESP message

// Example 2. RESP Message for GET favourite_food 

// *2\r\n$3\r\nGET\r\n$13\r\nfavourite_food\r\n
// *2 = Array of 2 elements (GET and favourite_food)
// $3 = Bulk string of length 3 (GET is a bulk string of length 3)
// GET = The actual command being sent by the client
// $13 = Bulk string of length 13 (favourite_food is a bulk string of length 13)
// favourite_food = The actual key being sent by the client
// \r\n = Carriage return and newline, used to indicate the end of a RESP message
#pragma once
#ifndef RESP_PARSER_H
#define RESP_PARSER_H

#include <vector>
#include <string>
#include <cstddef>
#include <stdexcept>

// 1. Keep the Enum
enum class RespType {
    Array,
    BulkString,
    SimpleString,
    Unknown
};

// 2. Keep the Data Structure
struct RespValue {
    RespType type = RespType::Unknown;
    std::vector<RespValue> elements; // Used for Arrays
    std::string bulkString;          // Used for Bulk Strings
};

// 3. RENAME THIS to RespParser
class RespParser {
public:
    static RespValue parse(const std::string& raw) {
        RespValue value; // Now this correctly refers to the struct above
        if (raw.empty()) return value;

        if (raw[0] == '*') {
            value.type = RespType::Array;
            size_t pos = raw.find("\r\n");
            
            // Basic error handling for stoi
            int num_elements = std::stoi(raw.substr(1, pos - 1));
            
            size_t current_pos = pos + 2;
            for (int i = 0; i < num_elements; ++i) {
                if (raw[current_pos] == '$') {
                    size_t next_crlf = raw.find("\r\n", current_pos);
                    int str_len = std::stoi(raw.substr(current_pos + 1, next_crlf - current_pos - 1));
                    
                    current_pos = next_crlf + 2;
                    
                    // Create a new RespValue for the element
                    RespValue element;
                    element.type = RespType::BulkString;
                    element.bulkString = raw.substr(current_pos, str_len);
                    
                    value.elements.push_back(element);
                    
                    current_pos += str_len + 2; // Move past content and \r\n
                }
            }
        }
        return value;
    }

    // Serialize a top-level RESP array of bulk strings (command shape from parse()).
    static std::string serialize_array(const RespValue& req) {
        std::string out;
        if (req.type != RespType::Array) {
            return out;
        }
        out += "*" + std::to_string(req.elements.size()) + "\r\n";
        for (const auto& el : req.elements) {
            const std::string& s = el.bulkString;
            out += "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
        }
        return out;
    }

    // If `raw` begins with a complete RESP array, fills `value` and sets `consumed`; otherwise returns false.
    static bool try_parse_complete_array(const std::string& raw, RespValue& value, std::size_t& consumed) {
        value = RespValue{};
        consumed = 0;
        if (raw.size() < 4 || raw[0] != '*') {
            return false;
        }
        const std::size_t pos = raw.find("\r\n", 1);
        if (pos == std::string::npos) {
            return false;
        }
        int num_elements = 0;
        try {
            num_elements = std::stoi(raw.substr(1, pos - 1));
        } catch (...) {
            return false;
        }
        if (num_elements < 0) {
            return false;
        }
        value.type = RespType::Array;
        std::size_t current_pos = pos + 2;
        for (int i = 0; i < num_elements; ++i) {
            if (current_pos >= raw.size() || raw[current_pos] != '$') {
                return false;
            }
            const std::size_t len_crlf = raw.find("\r\n", current_pos + 1);
            if (len_crlf == std::string::npos) {
                return false;
            }
            int str_len = 0;
            try {
                str_len = std::stoi(raw.substr(current_pos + 1, len_crlf - current_pos - 1));
            } catch (...) {
                return false;
            }
            current_pos = len_crlf + 2;
            if (str_len < -1) {
                return false;
            }
            RespValue element;
            element.type = RespType::BulkString;
            if (str_len == -1) {
                element.bulkString.clear();
                value.elements.push_back(std::move(element));
                continue;
            }
            if (raw.size() < current_pos + static_cast<std::size_t>(str_len) + 2) {
                return false;
            }
            element.bulkString = raw.substr(current_pos, static_cast<std::size_t>(str_len));
            value.elements.push_back(std::move(element));
            current_pos += static_cast<std::size_t>(str_len) + 2;
        }
        consumed = current_pos;
        return true;
    }
};

#endif // RESP_PARSER_H