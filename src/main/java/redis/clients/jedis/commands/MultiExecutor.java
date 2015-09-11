package redis.clients.jedis.commands;

import redis.clients.jedis.Transaction;

public interface MultiExecutor {
  void exec(String key, Transaction t);
}