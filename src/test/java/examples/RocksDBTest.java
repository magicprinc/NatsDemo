package examples;

import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static examples.MagicUtils.TEMP_DIR;
import static examples.MagicUtils.asLatin1;
import static examples.MagicUtils.execute;
import static examples.MagicUtils.loop;
import static examples.MagicUtils.now;
import static examples.MagicUtils.perfToString;
import static java.nio.charset.StandardCharsets.*;
import static org.junit.jupiter.api.Assertions.*;

/// https://github.com/facebook/rocksdb/tree/main/java
/// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics
/// https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning
/// https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-from-Java
///
/// todo see Ignite3 rocksDB settings!
///
/// есть pessimistic locking (для высокой конкуренции) и optimistic locking (CAS)
/// key < 8M, value <3G, ANY bytes!
///
/// ttl (один на всю БД) задаётся в момент открытия БД
///
/// Открыть ту же самую БД можно только для чтения (так делают backup replica)
///
/// SQLite?
public class RocksDBTest {
	static {
		RocksDB.loadLibrary();
	}

	static final int MAX = 5_000_000;

	@Test  @SneakyThrows
	void benchmark () throws RocksDBException {
		String dbPath = TEMP_DIR;
		System.out.println(dbPath);
		val options = new Options()
			.setCreateIfMissing(true)
			//.setMaxBackgroundJobs(Runtime.getRuntime().availableProcessors())// def 2
			//.setWriteBufferSize(128 * 1024 * 1024) // def 64MB memtable
			//.setCompactionStyle(CompactionStyle.LEVEL) // compaction_style: Level compaction (default) or Universal; Universal is sometimes faster for write-heavy workloads.
			.setCompressionType(CompressionType.ZSTD_COMPRESSION)
			.setUseFsync(false)// use_fsync: true for stronger durability guarantees (makes writes hit disk): If false, then every store to stable storage will issue a fdatasync. This parameter should be set to true while storing data to filesystem like ext3 that can lose files after a reboot.
			//.setMergeOperator(new UInt64AddOperator() / StringAppendOperator) нельзя написать свой на Java 🤷‍♀️
			;

//		// Using around 10 bits per key gives you a ~1% false positive rate, which optimizes CPU and block cache usage.
//		// The second BloomFilter constructor parameter, useBlockBasedBuilder, when set to false, enables the "full filter" format for faster lookups by avoiding multiple filter block lookups.
//		//    The "full filter" mode improves point lookup performance by simplifying Bloom filter checks to one per SST file.
//		val tableConfig = new BlockBasedTableConfig();
//		// Configure Bloom filter with 10 bits per key, disable block-based builder for a full filter (faster)
//		tableConfig.setFilterPolicy(new BloomFilter(10, false));
//		tableConfig.setCacheIndexAndFilterBlocks(true);// Enable caching of filter and index blocks to keep Bloom filters in the block cache for speedy access.
//		options.setTableFormatConfig(tableConfig);

		try (RocksDB db = RocksDB.open(options, dbPath)){
			System.out.println("1️⃣ Create 10 mi keys");

			long t = now();
			for (int i = 0; i < MAX; ){
				db.put(Long.toString(7900_000_00_00L + i).getBytes(ISO_8859_1), Long.toString(7900_000_00_00L + i).repeat(7).getBytes(ISO_8859_1));
				if (++i % 500_000 == 0) System.out.println(i);
			}
			System.out.println(perfToString(t, now(), MAX));


			System.out.println("2️⃣ Single thread sequential reads");
			t = now();
			for (int i = 0; i < MAX; ){
				var e = db.get(Long.toString(7900_000_00_00L + i).getBytes(ISO_8859_1));
				assertEquals(Long.toString(7900_000_00_00L + i).repeat(7), asLatin1(e));
				if (++i % 100_000 == 0) System.out.println(i);
			}
			System.out.println(perfToString(t, now(), MAX));


			System.out.println("3️⃣ Single thread random BATCH reads");
			t = now();
			val r = ThreadLocalRandom.current();
			val req = new ArrayList<byte[]>(50);
			for (int n = 0; n < MAX; ){
				req.clear();
				for (int j = 0; j < 50 && n < MAX; j++, n++){
					if (n % 100_000 == 0) System.out.println(n);
					int i = r.nextInt(0, MAX);
					req.add(Long.toString(7900_000_00_00L + i).getBytes(ISO_8859_1));
				}
				var e = db.multiGetAsList(req);// 🚀
				assertEquals(e.size(), req.size());

				for (int j = 0; j < req.size(); j++){
					assertEquals(asLatin1(req.get(j)).repeat(7), asLatin1(e.get(j)));
				}
			}
			System.out.println(perfToString(t, now(), MAX));


			System.out.println("4️⃣ Multi threads random reads 🚀...");
			t = now();
			val w = new CountDownLatch(10);
			val failure = new AtomicReference<Throwable>();
			loop(10, ()->execute(()->{
					try {
						for (int n = 0; n < MAX; ){
							int i = ThreadLocalRandom.current().nextInt(0, MAX);
							var e = db.get(Long.toString(7900_000_00_00L + i).getBytes(ISO_8859_1));
							assertEquals(Long.toString(7900_000_00_00L + i).repeat(7), asLatin1(e));
							if (++n % 100_000 == 0) System.out.println(n);
						}
						w.countDown();
					} catch (Throwable e){
						w.countDown();
						failure.set(e);
					}
				}));
			boolean done = w.await(15, TimeUnit.MINUTES);
			assertTrue(done);
			if (failure.get() != null) throw failure.get();
			System.out.println(perfToString(t, now(), MAX));
			System.out.println(perfToString(t, now(), MAX*10));
		}
	}
}