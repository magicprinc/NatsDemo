package com.devinotele.common.nats.component;

import lombok.val;
import org.jooq.lambda.Loops;
import org.jooq.lambda.MILLI;
import org.jooq.lambda.NANO;
import org.jooq.lambda.concurrent.JThread;
import org.jooq.lambda.conf.JProperties;
import org.jooq.lambda.test.Waiter;
import org.jooq.lambda.util.JIO;
import org.junit.jupiter.api.Test;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.concurrent.ThreadLocalRandom;

import static java.nio.charset.StandardCharsets.*;
import static org.jooq.lambda.JCore.asLatin1;
import static org.jooq.lambda.JCore.asStr;
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

	@Test
	void benchmark () {
		String dbPath = JIO.normPath(JProperties.TEMP_DIR.toString());
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

			// Put key-value
			db.put("key1".getBytes(), "value1".getBytes());

			// Get key-value
			byte[] value = db.get("key1".getBytes());
			System.out.println("Retrieved value: "+ ( value != null ? asStr(value) : "null" ));

			// Delete key
			db.delete("key1".getBytes());

			//1. Создадим 100 млн ключей
			long t = MILLI.now();
			for (int i = 0; i < 10_000_000; ){
				db.put(Long.toString(7900_000_00_00L + i).getBytes(ISO_8859_1), Long.toString(7900_000_00_00L + i).repeat(7).getBytes(ISO_8859_1));
				if (++i % 100_000 == 0){
					System.out.println(i);
				}
			}
			System.out.println(MILLI.toString(t, NANO.now(), 10_000_000));

			System.out.println("А теперь скорость чтения...");
			t = MILLI.now();
			for (int i = 0; i < 10_000_000; ){
				var e = db.get(Long.toString(7900_000_00_00L + i).getBytes(ISO_8859_1));
				assertEquals(Long.toString(7900_000_00_00L + i).repeat(7), asLatin1(e));
				if (++i % 100_000 == 0)
						System.out.println(i);
			}
			System.out.println(MILLI.toString(t, NANO.now(), 10_000_000));


//			System.out.println("А теперь скорость чтения СЛУЧАЙНЫХ чтений...");
//			t = MILLI.now();
//			var r = ThreadLocalRandom.current();
//			for (int n = 0; n < 10_000_000; ){
//				int i = r.nextInt(0, 10_000_000);
//				var e = db.get(Long.toString(7900_000_00_00L + i).getBytes(ISO_8859_1));
//				assertEquals(Long.toString(7900_000_00_00L + i).repeat(7), asLatin1(e));
//				if (++n % 100_000 == 0)
//						System.out.println(n);
//			}
//			System.out.println(MILLI.toString(t, NANO.now(), 10_000_000));

			System.out.println("А теперь скорость чтения СЛУЧАЙНЫХ чтений МНОГОПОТОЧНО 🚀...");
			t = MILLI.now();
			val w = Waiter.of();
			Loops.loop(10, ()->JThread.VT.execute(()->{
					try {
						for (int n = 0; n < 10_000_000; ){
							int i = ThreadLocalRandom.current().nextInt(0, 10_000_000);
							var e = db.get(Long.toString(7900_000_00_00L + i).getBytes(ISO_8859_1));
							assertEquals(Long.toString(7900_000_00_00L + i).repeat(7), asLatin1(e));
							if (++n % 100_000 == 0)
								System.out.println(n);
						}
						w.resume();
					} catch (Throwable e){
						w.fail(e);
					}
				}));
			w.await(999_000, 10);
			System.out.println(MILLI.toString(t, NANO.now(), 10_000_000));
			System.out.println(MILLI.toString(t, NANO.now(), 100_000_000));

		} catch (RocksDBException e){
			e.printStackTrace();
		}
	}
}