package examples;

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

//@Testcontainers
//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IgniteEmbeddedClusterPerformanceTest {

	private static Ignite ignite;
	private static IgniteCache<String, String> cache;

	@BeforeAll
	static void setupCluster() {
		IgniteConfiguration cfg = new IgniteConfiguration();
		cfg.setIgniteInstanceName("string-cache-test");

//		cfg.setWorkDirectory(null); // Disable persistence for better performance

		String dbPath = JIO.normPath(JProperties.TEMP_DIR.toString());
		System.out.println(dbPath);

		cfg.setWorkDirectory(dbPath); // Or another path
		DataStorageConfiguration storageCfg = new DataStorageConfiguration();
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
	static void tearDownCluster() {
		if (ignite != null) {
			ignite.close();
		}
	}

	@Test
	@DisplayName("Single-node put performance")
	void testSingleNodePutPerformance() {

		long t = MILLI.now();
		for (int i = 0; i < 1_000_000; i++){
			cache.put(Long.toString(7900_000_00_00L + i), Long.toString(7900_000_00_00L + i).repeat(7));
			if (i % 10_000 == 9_999){
				System.out.println(i);
			}
		}
		System.out.printf("Запись ____%s%n", MILLI.toString(t, NANO.now(), 1_000_000));

		t = MILLI.now();
		for (int i = 0; i < 1_000_000; i++){
			var e = cache.get(Long.toString(7900_000_00_00L + (int) (Math.random() * 1_000_000)));
			if (i % 10_000 == 9_999){
				System.out.println(i);
			}
		}
		System.out.printf("Чтение случайное ____%s%n", MILLI.toString(t, NANO.now(), 1_000_000));

	}

}