package examples;

import lombok.Cleanup;
import lombok.val;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static examples.MagicUtils.close;
import static examples.MagicUtils.now;
import static examples.MagicUtils.perfToString;
import static java.nio.charset.StandardCharsets.*;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RedisTest {
	@Container
	private static final GenericContainer<?> redisContainer =
		new GenericContainer<>(DockerImageName.parse("redis:latest"))
			.withExposedPorts(6379)
			.withCommand("redis-server", "--save", "", "--appendonly", "no");
//			.withNetworkMode("host");

	private static JedisPool jedisPool;
	private static Jedis jedis;
	private static final int THREAD_COUNT = 10;

	@BeforeAll
	static void setup() {
		String redisHost = redisContainer.getHost();
		int redisPort = redisContainer.getFirstMappedPort();

		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(THREAD_COUNT * 2);
		poolConfig.setMaxIdle(THREAD_COUNT);
		poolConfig.setMinIdle(2);
		poolConfig.setTestOnBorrow(true);
		poolConfig.setTestOnReturn(true);
		poolConfig.setTestWhileIdle(true);
		poolConfig.setMinEvictableIdleTime(Duration.ofSeconds(60));
		poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30));
		poolConfig.setNumTestsPerEvictionRun(3);
		poolConfig.setBlockWhenExhausted(true);

		jedisPool = new JedisPool(poolConfig, redisHost, redisPort);
		jedis = jedisPool.getResource();

		// Flush any existing data
		jedis.flushAll();

		System.out.println("Started REDIS");
	}

	@AfterAll
	static void tearDown() {
		close(jedis);
		close(jedisPool);
		close(redisContainer);
	}

	static final int MAX = 500_000;

	@Test  @DisplayName("Single-threaded write/read performance test")
	void testSingleThreadedWritePerformance() {
		long t = now();
		for (int i = 0; i < MAX; ){
			jedis.set(Long.toString(7900_000_00_00L + i).getBytes(ISO_8859_1), Long.toString(7900_000_00_00L + i).repeat(7).getBytes(ISO_8859_1));
			if (++i % 10_000 == 0) System.out.println(i);
		}
		System.out.println(perfToString(t, now(), MAX));

		t = now();
		val r = ThreadLocalRandom.current();
		for (int n = 0; n < MAX; ){
			int i = r.nextInt(0, MAX);
			var e = jedis.get(Long.toString(7900_000_00_00L + i));
			if (++n % 20_000 == 0) System.out.println(n);
			assertEquals(Long.toString(7900_000_00_00L + i).repeat(7), e);
		}
		System.out.println(perfToString(t, now(), MAX));
	}

	@Test  @DisplayName("Multithreaded write performance test")
	void testMultiThreadedWritePerformance() throws InterruptedException, ExecutionException {
		@Cleanup ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
		List<Future<Long>> futures = new ArrayList<>();

		long t = now();

		for (int i = 0; i < THREAD_COUNT; i++) {
			final int threadId = i;
			Callable<Long> task = () -> {
				try (Jedis threadJedis = jedisPool.getResource()){
					long operations = 0;
					for (int j = 0; j < 10_000; j++){
						threadJedis.set(String.format("mt_key_t%d_%d", threadId, j).getBytes(ISO_8859_1), Long.toString(7900_000_00_00L + j).repeat(7).getBytes(ISO_8859_1));
						operations++;
					}
					return operations;
				}
			};
			futures.add(executor.submit(task));
		}

		// Wait for all tasks to complete
		for (Future<Long> future : futures){
			future.get();
		}

		executor.shutdown();
		executor.awaitTermination(5, TimeUnit.SECONDS);

		System.out.println(perfToString(t, now(), 100_000));
	}
}