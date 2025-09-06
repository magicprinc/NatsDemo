package examples;

import lombok.val;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static examples.MagicUtils.close;
import static examples.MagicUtils.now;
import static examples.MagicUtils.perfToString;
import static org.junit.jupiter.api.Assertions.*;

/// https://hub.docker.com/r/apacheignite/ignite/
@Testcontainers
//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled
public class IgniteClientClusterPerformanceTest {
	@Container
	private static final GenericContainer<?> igniteContainer =
		new GenericContainer<>(DockerImageName.parse("apacheignite/ignite:2.17.0"))
			.withExposedPorts(10800, 47100, 11211,
				47500, 47501, 47502, 47503, 47504, 47505, 47506, 47507, 47508, 47509)
			.withEnv("IGNITE_QUIET", "false") // Optional: Enable logs to see startup issues
			;

	static IgniteClient ignite;
	static ClientCache<String, String> cache;
	static String containerHost;
	static int containerPort;

	@BeforeAll
	static void setupCluster () {
		containerHost = igniteContainer.getHost();
		containerPort = igniteContainer.getMappedPort(10800);

		System.out.println("👉 Connect to Ignite Cluster: " + containerHost + ":" + containerPort);
		val clientConfig = new ClientConfiguration()
			.setAddresses(containerHost + ":" + containerPort);

		ignite = Ignition.startClient(clientConfig);

		val cacheCfg = new ClientCacheConfiguration();
		cacheCfg.setName("string-performance-cache");
		cacheCfg.setBackups(0); // No backups for maximum performance
		cacheCfg.setStatisticsEnabled(false); // Disable stats for performance

		cache = ignite.getOrCreateCache(cacheCfg);
	}

	@AfterAll
	static void tearDownCluster () {
		close(ignite);
		close(igniteContainer);
	}

	@Test
	public void testCacheOperations() {
		var cache = ignite.getOrCreateCache("foo");
		cache.put("key1", "value1");
		assertEquals("value1", cache.get("key1"));
	}

	@Test @DisplayName("Single-node put performance")
	void testSingleNodePutPerformance () {
		System.out.println("===== Apache Ignite Cluster - testSingleNodePutPerformance ====");
		long t = now();
		for (int i = 0; i < 1_000_000; ){
			cache.put(Long.toString(7900_000_00_00L + i), Long.toString(7900_000_00_00L + i).repeat(7));
			if (++i % 10_000 == 0)
					System.out.println(i);
		}
		System.out.printf("Write ____%s%n", perfToString(t, now(), 1_000_000));

		t = now();
		for (int i = 0; i < 1_000_000; ){
			var e = cache.get(Long.toString(7900_000_00_00L + (int) (Math.random() * 1_000_000)));
			if (++i % 10_000 == 0)
					System.out.println(i);
		}
		System.out.printf("Random reads ____%s%n", perfToString(t, now(), 1_000_000));
	}
}