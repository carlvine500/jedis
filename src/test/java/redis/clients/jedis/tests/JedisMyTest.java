package redis.clients.jedis.tests;

import redis.clients.jedis.Jedis;

public class JedisMyTest {

  /**
   * @param args
   */
  public static void main(String[] args) {
    Jedis jedis = new Jedis("10.126.53.98", 7000);
    jedis.get("a");
    jedis.close();
    System.out.println(jedis.get("a"));
  }

}
