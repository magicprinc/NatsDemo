package examples.rocksdb;

import com.google.common.base.Verify;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import examples.MagicUtils;
import lombok.SneakyThrows;
import lombok.val;
import org.jspecify.annotations.Nullable;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TtlDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static examples.MagicUtils.close;
import static java.nio.charset.StandardCharsets.*;

public final class RocksDBConfig {
	static {
		RocksDB.loadLibrary();
	}
	static final int DEF_TTL = (int) TimeUnit.DAYS.toSeconds(7);
	public static final String DEFAULT = new String(RocksDB.DEFAULT_COLUMN_FAMILY, UTF_8);

	private final RocksDB rocksDB;
	final LRUCache blockCache = new LRUCache(512 * 1024 * 1024L, 16); // 512MB block cache, 8 shards
	final List<ColumnFamilyDescriptor> cfDescriptors;
	private final Map<String,ColumnFamilyHandle> columnFamilyHandles = new LinkedHashMap<>();

	/// @see org.rocksdb.TtlDB
	/// @see org.rocksdb.TransactionDB
	/// @see org.rocksdb.OptimisticTransactionDB
	@SneakyThrows
	RocksDBConfig () {
		// Create a database directory if it doesn't exist
		val path = Files.createTempDirectory("rocksdb");// from config

		BlockBasedTableConfig tableConfig = createTableConfig();

		// columnFamilies ~ БД внутри СУБД
		val cf = Set.of(DEFAULT, "harry", "potter");
		cfDescriptors = cf.stream()
				.map(columnFamilyName -> new ColumnFamilyDescriptor(
						columnFamilyName.getBytes(UTF_8),
						createColumnFamilyOptions(columnFamilyName, tableConfig)
				))
				.toList();

		val dbOptions = new DBOptions() // vs Options 👀
			.setCreateIfMissing(true)
			.setCreateMissingColumnFamilies(true)
			.setMaxBackgroundJobs(Runtime.getRuntime().availableProcessors())
			.setUseFsync(false)// use_fsync: true for stronger durability guarantees (makes writes hit disk): If false, then every store to stable storage will issue a fdatasync. This parameter should be set to true while storing data to filesystem like ext3 that can lose files after a reboot.
			//.setMergeOperator(new UInt64AddOperator() / StringAppendOperator) not using Java 🤷‍♀️
			.setUseDirectIoForFlushAndCompaction(true)
			//.setMaxTotalWalSize() ?

			.setStatsDumpPeriodSec(10)// doesn't work?
			;

		rocksDB = createRocksDB(dbOptions, path.toFile().getAbsolutePath(), cf);
		Runtime.getRuntime().addShutdownHook(new Thread("RocksDBShutdownHook"){
			@Override public void run (){ shutdown(); }
		});
	}//new

	private RocksDB createRocksDB (DBOptions dbOptions, String pathToDb, Set<String> cf) throws RocksDBException {
		final RocksDB db;
		// a list which will hold the handles for the column families once the db is opened
		val handles = new ArrayList<ColumnFamilyHandle>();

		List<Integer> ttlList = cf.stream()
				.map(columnFamilyName->DEF_TTL)
				.toList();

		db = TtlDB.open(
			dbOptions,
			pathToDb,
			cfDescriptors,
			handles,
			ttlList,
			false // not readOnly
		);

		// Map columnFamilyName→ColumnFamilyHandle
		for (int i = 0; i < handles.size(); i++){
			String cfName = new String(cfDescriptors.get(i).getName(), UTF_8);
			columnFamilyHandles.put(cfName, handles.get(i));
		}

		return db;
	}

	void shutdown () {
		System.err.println("⛔ RocksDB is closing ".repeat(5));
		Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

		rocksDB.cancelAllBackgroundWork(true); // Wait for BG jobs
		columnFamilyHandles.values().forEach(MagicUtils::close);
		cfDescriptors.forEach(d->close(d.getOptions()));
		close(blockCache);
		close(rocksDB);
	}

	public static RocksDBConfig conf () {
		return SingletonHolder.rocksDBConfig;
	}
	private static final class SingletonHolder {
		private static final RocksDBConfig rocksDBConfig = new RocksDBConfig();
	}

	@CanIgnoreReturnValue
	public static RocksDB db (){ return conf().rocksDB; }

	public static @Nullable ColumnFamilyHandle getHandleOrNull (String columnFamilyName) {
		return conf().columnFamilyHandles.get(columnFamilyName);
	}

	public static ColumnFamilyHandle getHandle (String columnFamilyName) throws IllegalArgumentException {
		ColumnFamilyHandle handle = getHandleOrNull(columnFamilyName);
		if (handle != null)
				return handle;
		else
				throw new IllegalArgumentException("getHandle: columnFamilyName NOT found: "+ columnFamilyName);
	}

	public Set<String> getColumnFamilyNames () {
		return Collections.unmodifiableSet(columnFamilyHandles.keySet());
	}


	/// todo one [ColumnFamilyOptions] for all CF?
	private static ColumnFamilyOptions createColumnFamilyOptions (String columnFamilyName4confOpts, BlockBasedTableConfig createTableConfig) {
		long writeBufferSize = 128;

		val options = new ColumnFamilyOptions()
			.optimizeLevelStyleCompaction()
			//.optimizeLevelStyleCompaction(128 << 20)

			.setCompressionType(CompressionType.ZSTD_COMPRESSION)
			.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
			// Optimize for long keys and sequential access
			.setOptimizeFiltersForHits(true)
			.setWriteBufferSize(writeBufferSize * 1024 * 1024)// 128 MB per CF

			.setForceConsistencyChecks(true)

			//.setLevelCompactionDynamicLevelBytes(true)
			.setMaxWriteBufferNumber(4)
			.setMinWriteBufferNumberToMerge(2)
			//.setLevel0FileNumCompactionTrigger(6)
			//.setPeriodicCompactionSeconds(xxx)

			.setTableFormatConfig(createTableConfig);

		return options;
	}

	/// Create shared block cache & Configure Block-Based Table for caching
	private BlockBasedTableConfig createTableConfig () {
		val tableConfig = new BlockBasedTableConfig();

		if (System.getProperty("blockCache") != null){
			tableConfig.setBlockCache(blockCache);
		} else {
			tableConfig.setBlockCacheSize(512 * 1024 * 1024);
		}

		//tableConfig.setCacheIndexAndFilterBlocks(true);
		//tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
		tableConfig.setBlockSize(16 * 1024);// Default: 4KB - optimal for point gets
		tableConfig.setFilterPolicy(new BloomFilter());// 10, false
		// Consider partitioned filters for large datasets
		//tableConfig.setPartitionFilters(true);
		//tableConfig.setMetadataBlockSize(4096);// 4k is default
		return tableConfig;
	}

	public static long approxCount (@Nullable ColumnFamilyHandle cfh) {
		try {
			return db().getLongProperty(cfh, "rocksdb.estimate-num-keys");// usually wrong
		} catch (RocksDBException e){
			return -1;
		}
	}

	/// "old" t =  64_588 ~ 77_413,76 op/s
	/// "new" t = 12_6040 ~ 39_669,95 op/s
	public static void main (String[] args) throws RocksDBException {
		//System.setProperty("blockCache", "true");// -DblockCache=true

		ColumnFamilyHandle h = getHandle("harry");

		long t = System.currentTimeMillis();

		int max = 5_000_000;

		ForkJoinPool.commonPool().execute(()->{
			while (true){
				try {
					db().get(h, intToBytes(ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE)));
				} catch (RocksDBException e){
					return;
				}
			}
		});
		ForkJoinPool.commonPool().execute(()->{
			while (true){
				try {
					int i = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE/2);
					db().put(h, intToBytes(i), intToBytes(i*2));
				} catch (RocksDBException e){
					return;
				}
			}
		});


		for (int i = 0; i < max; i++){
			db().put(h, intToBytes(i), intToBytes(i*2));

			if (i% 20_000 == 0){
				System.out.println(i);
			}
		}
		for (int i = 0; i < max; i++){
			byte[] b = db().get(h, intToBytes(i));
			Verify.verifyNotNull(b);
			Verify.verify(Arrays.equals(b, intToBytes(i*2)));
		}

		t = System.currentTimeMillis() - t;
		System.out.printf("t = %d ~ %.2f op/s%n", t, max*1000.0/t);
	}

	public static byte[] intToBytes (int value) {
		return ByteBuffer.allocate(4)
			.order(ByteOrder.BIG_ENDIAN)
			.putInt(value)
			.array();
	}
}