// Unit tests for parser and small data helpers (no server, no sockets).
//
// Build (from repo root):
//   g++ -std=c++23 -o redis_unit_test test/test.cpp -Isrc
// Or: cmake --build build --target redis_unit_test

#include <iostream>
#include <string>

#include "dataStructures.h"
#include "respparser.h"

namespace {

int g_failures = 0;

void check(const char* name, bool ok) {
    if (ok) {
        std::cout << " [PASS] " << name << '\n';
    } else {
        std::cout << " [FAIL] " << name << '\n';
        ++g_failures;
    }
}

} // namespace

int main() {
    std::cout << "--- unit tests (parser + data helpers) ---\n";

    // RespParser::parse — GET with key
    {
        const std::string raw = "*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n";
        const RespValue v = RespParser::parse(raw);
        check("parse array type", v.type == RespType::Array && v.elements.size() == 2);
        check("parse GET command", v.elements[0].bulkString == "GET");
        check("parse GET key", v.elements[1].bulkString == "key1");
    }

    // RespParser::serialize_array round-trip
    {
        RespValue cmd;
        cmd.type = RespType::Array;
        RespValue a;
        a.type = RespType::BulkString;
        a.bulkString = "SET";
        RespValue b;
        b.type = RespType::BulkString;
        b.bulkString = "k";
        RespValue c;
        c.type = RespType::BulkString;
        c.bulkString = "v";
        cmd.elements = {a, b, c};
        const std::string wire = RespParser::serialize_array(cmd);
        const RespValue again = RespParser::parse(wire);
        check("serialize round-trip count", again.elements.size() == 3);
        check("serialize round-trip payload",
              again.elements[0].bulkString == "SET" && again.elements[1].bulkString == "k" &&
              again.elements[2].bulkString == "v");
    }

    // try_parse_complete_array — full message
    {
        RespValue v;
        std::size_t consumed = 0;
        const std::string raw = "*1\r\n$4\r\nPING\r\n";
        const bool ok = RespParser::try_parse_complete_array(raw, v, consumed);
        check("try_parse complete", ok && consumed == raw.size() && v.elements.size() == 1 &&
                                    v.elements[0].bulkString == "PING");
    }

    // try_parse_complete_array — truncated (incomplete)
    {
        RespValue v;
        std::size_t consumed = 0;
        const std::string raw = "*1\r\n$4\r\nPI";
        check("try_parse incomplete", !RespParser::try_parse_complete_array(raw, v, consumed));
    }

    // try_parse_complete_array — null bulk
    {
        RespValue v;
        std::size_t consumed = 0;
        const std::string raw = "*1\r\n$-1\r\n";
        const bool ok = RespParser::try_parse_complete_array(raw, v, consumed);
        check("try_parse null bulk", ok && v.elements.size() == 1 && v.elements[0].bulkString.empty());
    }

    // typeToString
    check("typeToString string", typeToString(KeyType::String) == "string");
    check("typeToString stream", typeToString(KeyType::Stream) == "stream");
    check("typeToString none default", typeToString(KeyType::None) == "none");

    // StreamID
    {
        const StreamID id = StreamID::parse("10-5");
        check("StreamID::parse", id.ms == 10 && id.seq == 5);
    }
    {
        const StreamID minus = StreamID::parseRange("-", true);
        check("StreamID parseRange -", minus.ms == 0 && minus.seq == 0);
        const StreamID plus = StreamID::parseRange("+", false);
        check("StreamID parseRange +", plus.ms == LLONG_MAX);
    }
    {
        StreamID a{1, 0};
        StreamID b{1, 1};
        check("StreamID order", a < b && b > a && a.toString() == "1-0");
    }

    std::cout << "--- done (" << g_failures << " failure(s)) ---\n";
    return g_failures == 0 ? 0 : 1;
}
