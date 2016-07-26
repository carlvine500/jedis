package redis.clients.jedis.commands;

import redis.clients.jedis.Pipeline;

public interface PipelineExecutor<Type> {
  void exec(Type key, Pipeline p);
}