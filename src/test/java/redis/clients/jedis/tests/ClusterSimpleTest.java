// package redis.clients.jedis.tests;
//
// import java.util.Arrays;
// import java.util.HashSet;
// import java.util.Set;
// import java.util.concurrent.atomic.AtomicLong;
//
// import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
//
// import redis.clients.jedis.HostAndPort;
// import redis.clients.jedis.JedisCluster;
// import redis.clients.jedis.loadbanlance.RandomLoadBanlance;
//
// import com.taobao.stresstester.StressTestUtils;
// import com.taobao.stresstester.core.StressTask;
//
// public class ClusterSimpleTest {
//
// /**
// * @param args
// * @throws Exception
// */
// public static void main(String[] args) throws Exception {
// Set<HostAndPort> jedisClusterNode = new HashSet<HostAndPort>();
// jedisClusterNode.add(new HostAndPort("10.58.56.91", 8301));
//
// GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
// poolConfig.setMinIdle(128);
// poolConfig.setMaxIdle(128);
// poolConfig.setMaxTotal(128);
// poolConfig.setTestWhileIdle(true);
// final JedisCluster jc = new JedisCluster(jedisClusterNode, poolConfig);
// jc.setClusterLoadBanlance(new RandomLoadBanlance(1, 1));
// // Thread.sleep(1000000000000L);
// String s = jc.get("b");
// final AtomicLong i = new AtomicLong();
// StressTestUtils.testAndPrint(800, 100 * 10000, new StressTask() {
// @Override
// public Object doTask() throws Exception {
// jc.set(i.incrementAndGet() + "", "xxx");
// return null;
// }
// });
// }
//
// public static void testname2() throws Exception {
// testname1();
// }
//
// public static void testname1() throws Exception {
// if ("key".equals("")) {
// System.out.println(Arrays.toString(new Throwable().getStackTrace()));
// }
//
// }
//
// }
