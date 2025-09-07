package examples;

import lombok.val;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static examples.MagicUtils.TEMP_DIR;
import static examples.MagicUtils.close;
import static examples.MagicUtils.now;
import static examples.MagicUtils.perfToString;
import static org.junit.jupiter.api.Assertions.*;

/// https://ignite.apache.org/docs/ignite2/latest/
/// use vm.options
//@Testcontainers
//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IgniteEmbeddedClusterPerformanceTest {
	private static Ignite ignite;
	private static IgniteCache<String, String> cache;

	@BeforeAll
	static void setupCluster() {
		IgniteConfiguration cfg = new IgniteConfiguration();
		cfg.setIgniteInstanceName("string-cache-test");
		// cfg.setWorkDirectory(null); // Disable persistence for better performance

		System.out.println(TEMP_DIR);

		cfg.setWorkDirectory(TEMP_DIR);// Or another path
		val storageCfg = new DataStorageConfiguration();
		storageCfg.setStoragePath("ignitedb/storage");
		storageCfg.setWalPath("ignitedb/wal");
		storageCfg.setWalArchivePath("ignitedb/wal/archive");

		// Enable persistence for the default data region
		storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

		cfg.setDataStorageConfiguration(storageCfg);

		TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
		TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
		ipFinder.setAddresses(List.of("127.0.0.1:47500"));
		discoverySpi.setIpFinder(ipFinder);
		cfg.setDiscoverySpi(discoverySpi);

		ignite = Ignition.start(cfg);
		ignite.cluster().state(ClusterState.ACTIVE);

		// Create cache with optimized configuration
		CacheConfiguration<String, String> cacheCfg = new CacheConfiguration<>();
		cacheCfg.setName("string-performance-cache");
		cacheCfg.setBackups(0); // No backups for maximum performance
		cacheCfg.setStatisticsEnabled(false); // Disable stats for performance

		cache = ignite.getOrCreateCache(cacheCfg);
	}

	@AfterAll
	static void tearDownCluster () {
		close(ignite);
	}

	static final int MAX = 1_000_000;

	@Test  @DisplayName("Single-node put performance")
	void testSingleNodePutPerformance() {
		long t = now();
		for (int i = 0; i < MAX; ){
			cache.put(Long.toString(7900_000_00_00L + i), Long.toString(7900_000_00_00L + i).repeat(7));
			if (++i % 10_000 == 9_999) System.out.println(i);
		}
		System.out.printf("Write ____%s%n", perfToString(t, now(), MAX));

		t = now();
		val r = ThreadLocalRandom.current();
		for (int n = 0; n < MAX; ){
			var key = Long.toString(7900_000_00_00L + r.nextInt(MAX));
			var e = cache.get(key);
			if (++n % 10_000 == 0)
					System.out.println(n);
			assertEquals(key.repeat(7), e);
		}
		System.out.printf("Read random ____%s%n", perfToString(t, now(), MAX));
	}
}