package redis.clients.jedis.commands;

import redis.clients.jedis.Transaction;

public interface MultiExecutor<Type> {
  void exec(Type key, Transaction t);
}