package redis.clients.jedis.tests;

import redis.clients.jedis.Jedis;

public class JedisMyTest {

  /**
   * @param args
   */
  public static void main(String[] args) {
    Jedis jedis = new Jedis("10.144.34.97", 5011);
    for (int j = 0; j < 1000; j++) {
      for (int i = 0; i < 600; i++) {
        jedis.lpush("x" + j, "abcdefg" + i);
      }
    }

    jedis.close();
    System.out.println(jedis.get("a"));
  }

}
