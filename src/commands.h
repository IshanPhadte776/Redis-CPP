#pragma once
#ifndef COMMANDS_H
#define COMMANDS_H

#include "respparser.h"

#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <string>

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

// Transactions (optimistic locking): per-client watched keys live in main's handle_client.
void handle_watch(int fd, const RespValue& req, std::unordered_set<std::string>& watched_keys);

void execute_command(int client_fd, const RespValue& request);

// Used by MULTI/EXEC: guarantees exactly one RESP reply per queued command (errors inline in the EXEC array).
void execute_command_for_exec(int client_fd, const RespValue& request);

#endif // COMMANDS_H