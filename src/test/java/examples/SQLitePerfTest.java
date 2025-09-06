package examples;

import com.google.common.base.Verify;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConnection;
import org.sqlite.SQLiteDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

import static examples.MagicUtils.TEMP_DIR;
import static examples.MagicUtils.close;
import static examples.MagicUtils.execute;
import static examples.MagicUtils.loop;
import static examples.MagicUtils.now;
import static examples.MagicUtils.perfToString;
import static org.junit.jupiter.api.Assertions.*;

/// https://sqlite.org/
/// https://github.com/xerial/sqlite-jdbc
/// op/sec
/// batch insert 441_345
/// single random read 58_888
/// select table 1_108_156
/// multi thread random read 58_665
@Slf4j
public class SQLitePerfTest {
	static final int MAX = 5_000_000;

	@Test  @SneakyThrows
	void singleThread () {
		SQLiteConnection con = null;
		try {
			val dataSource = new SQLiteDataSource();// create a database connection
			SQLiteConfig cfg = dataSource.getConfig();
			dataSource.setUrl("jdbc:sqlite:/%s/test.sqlite".formatted(TEMP_DIR));// or file::memory:?cache=shared -or- jdbc:sqlite:file::memory:?cache=shared ~ 763k row/s
			//dataSource.setLoadExtension(true); //getConfig().enableLoadExtension();
			dataSource.setEnforceForeignKeys(true);
			dataSource.setSharedCache(true);// inside app's threads 👍
			dataSource.setCaseSensitiveLike(false);// only US-ASCII 😭
			//× dataSource.setCountChanges(true) → as ResultSet!
			cfg.setJournalMode(SQLiteConfig.JournalMode.WAL);//!!!
			dataSource.setTempStore("MEMORY");// org.sqlite.SQLiteConfig.TempStore.MEMORY → for temp files only, not the main storage
			cfg.setSynchronous(SQLiteConfig.SynchronousMode.NORMAL);// OFF faster, but risky; FULL slow and too "safe"
			cfg.setBusyTimeout(129_000);
			//cfg.setPragma(SQLiteConfig.Pragma.MMAP_SIZE, str(512*1024*1024));// 256MB https://www.sqlite.org/mmap.html  🤷‍♀️
			con = (SQLiteConnection) dataSource.getConnection();

			Statement statement = con.createStatement();
			statement.setQueryTimeout(120);// set timeout to 120 sec == conn.setBusyTimeout(1000 * seconds);
			statement.executeUpdate("drop table if exists keyvalue");
			statement.executeUpdate("create table keyvalue ( id VARCHAR PRIMARY KEY NOT NULL, value varchar)");

			val psInsert = con.prepareStatement("insert into keyvalue values(?, ?)");
			val psRead = con.prepareStatement("select value from keyvalue where id=?");
			psRead.setQueryTimeout(120);
			psRead.setMaxRows(1);
			con.setAutoCommit(false);//BEGIN TRAN

			System.out.println("SQLite single thread BATCH 5k insert...");
			long t = now();
			for (int i = 0; i < MAX; ){
				psInsert.setString(1, Long.toString(7900_000_00_00L + i));
				psInsert.setString(2, Long.toString(7900_000_00_00L + i).repeat(7));
				psInsert.addBatch();//+
				if (++i % 5000 == 0)
						commitBatch(psInsert, 5000);//+=>
				if (i % 500_000 == 0) System.out.println(i);
			}
			System.out.println(perfToString(t, now(), MAX));

			con.setAutoCommit(true);// commits tx

			System.out.println("SQLite single thread random read...");
			t = now();
			for (int n = 0; n < MAX; ){
				int i = ThreadLocalRandom.current().nextInt(0, MAX);
				psRead.setString(1, Long.toString(7900_000_00_00L + i));
				try (var rs = psRead.executeQuery()){
					rs.next();
					assertEquals(Long.toString(7900_000_00_00L + i).repeat(7), rs.getString(1));
					if (++n % 200_000 == 0) System.out.println(n);
				}
			}
			System.out.println(perfToString(t, now(), MAX));

			System.out.println("SQLite single thread BATCH (whole table) read...");
			t = now();
			val stRead = con.prepareStatement("select id, value from keyvalue order by id");
			try (var rs = stRead.executeQuery()){
				for (int i = 0; i < MAX; ){
					rs.next();
					assertEquals(Long.toString(7900_000_00_00L + i), rs.getString(1));
					assertEquals(Long.toString(7900_000_00_00L + i).repeat(7), rs.getString(2));
					if (++i % 500_000 == 0) System.out.println(i);
				}
			}
			System.out.println(perfToString(t, now(), MAX));

			System.out.println("SQLite multi-thread random read...");
			val bq = new LinkedBlockingQueue<>();
			val _con = con;
			t = now();
			loop(10, ()->execute(()->{
				try {
					val ps = _con.prepareStatement("select value from keyvalue where id=?");
					ps.setQueryTimeout(120);
					ps.setMaxRows(1);
					for (int n = 0; n < MAX; ){
						int i = ThreadLocalRandom.current().nextInt(0, MAX);
						ps.setString(1, Long.toString(7900_000_00_00L + i));
						try (var rs = ps.executeQuery()){
							rs.next();
							assertEquals(Long.toString(7900_000_00_00L + i).repeat(7), rs.getString(1));
							if (++n % 50_000 == 0) System.out.println(n);
						}
					}
					bq.offer("OK");
				} catch (SQLException e){
					bq.offer(e);
				}
			}));
			for (int i = 0; i < 10; i++){
				var o = bq.take();
				if (o instanceof Exception e) throw e;
			}
			System.out.println(perfToString(t, now(), MAX*10));
		} finally {
			close(con);
		}
	}

	static void commitBatch (PreparedStatement ps, int batchSize) throws SQLException {
		if (batchSize <= 0){ return; }
		int[] updates = ps.executeBatch();
		if (batchSize != updates.length)// ps.getUpdateCount(), × ps.getResultSet()
				log.error("batchSize != updates.length: {} != {}: {}", batchSize, updates.length, Arrays.toString(updates));
		Arrays.stream(updates).forEach(u->Verify.verify(1 == u));
		ps.getConnection().commit();//!!!
	}

	static void showResultSet (ResultSet rs) throws SQLException {
		int colCnt = rs.getMetaData().getColumnCount();
		while (rs.next()){// read the result set rows
			val sb = new StringBuilder(100);
			for (int i=1; i <= colCnt; i++)
					sb.append(rs.getString(i)).append('\t');
			log.info(sb.toString());// log(rs.getString("name")+'\t'+rs.getInt("id"));
		}
		rs.close();
	}
}