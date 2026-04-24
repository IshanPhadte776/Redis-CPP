#pragma once
#ifndef COMMANDS_H
#define COMMANDS_H

#include "respparser.h"

#include <functional>
#include <unordered_map>
#include <string>
#include <cstdint>
#include <vector>

// Core Commands
void handle_ping(int fd, const RespValue& req);
void handle_echo(int fd, const RespValue& req);
void handle_flushall(int fd, const RespValue& req);

// String Commands
void handle_set(int fd, const RespValue& req);
void handle_get(int fd, const RespValue& req);
void handle_incr(int fd, const RespValue& req);

// List Commands
void handle_rpush(int fd, const RespValue& req);
void handle_lpush(int fd, const RespValue& req);
void handle_lrange(int fd, const RespValue& req);
void handle_llen(int fd, const RespValue& req);
void handle_lpop(int fd, const RespValue& req);
void handle_blpop(int fd, const RespValue& req);

// Stream Commands
void handle_xadd(int fd, const RespValue& req);
void handle_xrange(int fd, const RespValue& req);
void handle_xread(int fd, const RespValue& req);

// Generic Commands
void handle_type(int fd, const RespValue& req);

// Transactions (optimistic locking): per-client state lives in main's handle_client.
void handle_watch(int fd, const RespValue& req,
                  std::unordered_map<std::string, std::uint64_t>& watch_versions,
                  std::uint64_t& watch_flush_epoch);

void handle_unwatch(int fd, std::unordered_map<std::string, std::uint64_t>& watch_versions,
                     std::uint64_t& watch_flush_epoch);

// Validates WATCH with store_mutex held, then runs queued commands (lock released so BLPOP can wait).
void execute_transaction_exec(int client_fd, std::vector<RespValue>& command_queue,
                              std::unordered_map<std::string, std::uint64_t>& watch_versions,
                              std::uint64_t& watch_flush_epoch);

// store_bump_key_revision: must be called with store_mutex held.
// store_note_database_flush: increments global flush counter; call with store_mutex held.
void store_bump_key_revision(const std::string& key);
void store_note_database_flush();

void execute_command(int client_fd, const RespValue& request);

// Used by MULTI/EXEC: guarantees exactly one RESP reply per queued command (errors inline in the EXEC array).
void execute_command_for_exec(int client_fd, const RespValue& request);

#endif // COMMANDS_H