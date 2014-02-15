package redis.clients.jedis.tests;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.DebugParams;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

public class JedisSentinelPoolTest extends JedisTestBase {
    private static final String MASTER_NAME = "mymaster";

    protected static HostAndPort master = HostAndPortUtil.getRedisServers()
	    .get(2);
    protected static HostAndPort slave1 = HostAndPortUtil.getRedisServers()
	    .get(3);
    protected static HostAndPort slave2 = HostAndPortUtil.getRedisServers()
	    .get(4);
    protected static HostAndPort sentinel1 = HostAndPortUtil
	    .getSentinelServers().get(1);

    protected static Jedis sentinelJedis1;

    protected Set<String> sentinels = new HashSet<String>();

    @Before
    public void setUp() throws Exception {
	sentinels.add(sentinel1.toString());

	sentinelJedis1 = new Jedis(sentinel1.getHost(), sentinel1.getPort());
    }

    @Test
    public void ensureSafeTwiceFailover() throws InterruptedException {
	JedisSentinelPool pool = new JedisSentinelPool(MASTER_NAME, sentinels,
		new GenericObjectPoolConfig(), 1000, "foobared", 2);

	forceFailover(pool);
	forceFailover(pool);
	
	// you can test failover as much as possible
    }

    private void forceFailover(JedisSentinelPool pool) throws InterruptedException {
	HostAndPort oldMaster = pool.getCurrentHostMaster();

	// jedis connection should be master
	Jedis jedis = pool.getResource();
	assertEquals("PONG", jedis.ping());

	// It can throw JedisDataException while there's no slave to promote
	// There's nothing we can do, so we just pass Exception to make test fail fast
	sentinelJedis1.sentinelFailover(MASTER_NAME);

	waitForFailover(pool, oldMaster);
	// JedisSentinelPool recognize master but may not changed internal pool yet
	Thread.sleep(100);

	jedis = pool.getResource();
	assertEquals("PONG", jedis.ping());
	assertEquals("foobared", jedis.configGet("requirepass").get(1));
	assertEquals(2, jedis.getDB().intValue());
    }

    private void waitForFailover(JedisSentinelPool pool, HostAndPort oldMaster)
	    throws InterruptedException {
	waitForJedisSentinelPoolRecognizeNewMaster(pool);
    }

    private void waitForJedisSentinelPoolRecognizeNewMaster(
	    JedisSentinelPool pool) throws InterruptedException {

	final AtomicReference<String> newmaster = new AtomicReference<String>(
		"");

	sentinelJedis1.psubscribe(new JedisPubSub() {

	    @Override
	    public void onMessage(String channel, String message) {
	    }

	    @Override
	    public void onPMessage(String pattern, String channel,
		    String message) {
		if (channel.equals("+switch-master")) {
		    newmaster.set(message);
		    punsubscribe();
		} else if (channel.startsWith("-failover-abort")) {
		    punsubscribe();
		    fail("Unfortunately sentinel cannot failover... reason(channel) : " + 
			    channel + " / message : " + message);
		}
	    }

	    @Override
	    public void onSubscribe(String channel, int subscribedChannels) {
	    }

	    @Override
	    public void onUnsubscribe(String channel, int subscribedChannels) {
	    }

	    @Override
	    public void onPUnsubscribe(String pattern, int subscribedChannels) {
	    }

	    @Override
	    public void onPSubscribe(String pattern, int subscribedChannels) {
	    }
	}, "*");

	String[] chunks = newmaster.get().split(" ");
	HostAndPort newMaster = new HostAndPort(chunks[3],
		Integer.parseInt(chunks[4]));

	while (true) {
	    String host = pool.getCurrentHostMaster().getHost();
	    int port = pool.getCurrentHostMaster().getPort();

	    if (host.equals(newMaster.getHost()) && port == newMaster.getPort())
		break;

	    System.out
		    .println("JedisSentinelPool's master is not yet changed, sleep...");

	    Thread.sleep(100);
	}
    }

    @Test
    public void returnResourceShouldResetState() {
	GenericObjectPoolConfig config = new GenericObjectPoolConfig();
	config.setMaxTotal(1);
	config.setBlockWhenExhausted(false);
	JedisSentinelPool pool = new JedisSentinelPool(MASTER_NAME, sentinels,
		config, 1000, "foobared", 2);

	Jedis jedis = pool.getResource();
	jedis.set("hello", "jedis");
	Transaction t = jedis.multi();
	t.set("hello", "world");
	pool.returnResource(jedis);

	Jedis jedis2 = pool.getResource();
	assertTrue(jedis == jedis2);
	assertEquals("jedis", jedis2.get("hello"));
	pool.returnResource(jedis2);
	pool.destroy();
    }
}
