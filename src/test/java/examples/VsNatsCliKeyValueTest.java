package examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueStatus;
import io.nats.client.api.StorageType;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import static examples.MagicUtils.close;
import static examples.MagicUtils.now;
import static examples.MagicUtils.perfToString;
import static org.junit.jupiter.api.Assertions.*;

/// https://docs.nats.io/nats-concepts/jetstream/key-value-store \
/// https://natsbyexample.com/examples/kv/intro/java \
/// https://github.com/nats-io/nats.go/discussions/1507#discussioncomment-14312986 \
/// `nats bench kv put --size="128" --msgs 1000000 --storage file` \
///  → 1m3s → Pub stats: 15,656 msgs/sec ~ 1.91 MB/sec \
/// `nats bench kv get --size="128" --msgs 1000000` \
///  → 59s → Sub stats: 16,720 msgs/sec ~ 2.04 MB/sec \
///
/// localhost NATS (not docker)
/// write 14_400
/// read
@Slf4j
class VsNatsCliKeyValueTest {
	private static Connection nc;

	@BeforeAll
	static void setup () throws Exception {
		Options options = new Options.Builder()
			.server("nats://localhost")
			.connectionTimeout(Duration.ofSeconds(30))
			.build();

		nc = Nats.connectReconnectOnConnect(options);
	}

	@AfterAll
	static void tearDown () {
		close(nc);
	}

	static final int MAX = 500_000;

	@Test
	void benchmark () throws IOException, JetStreamApiException {
		System.out.printf("1️⃣ Single-Thread create keys: %d%n", MAX);

		KeyValueManagement kvm = nc.keyValueManagement();

		KeyValueConfiguration kvc = KeyValueConfiguration.builder()
			.name("JNatsKeyValueStore-benchmark")// . нельзя ⇒ JetStream KV_JNatsKeyValueStore-benchmark
			.compression(true)
			.storageType(StorageType.File)
			.maxHistoryPerKey(1)
			.build();

		KeyValueStatus keyValueStatus = kvm.create(kvc);
		System.out.println(keyValueStatus);

		// Retrieve the Key Value context once the bucket is created:
		KeyValue kv = nc.keyValue("JNatsKeyValueStore-benchmark");

		long t = now();
		for (int i = 0; i < MAX; ){
			kv.put(Long.toString(7900_000_00_00L + i), Long.toString(7900_000_00_00L + i).repeat(7));
			if (++i % 10_000 == 0) System.out.println(i);
		}
		System.out.println(perfToString(t, now(), MAX));


		System.out.println("2️⃣ Single thread random reads");
		t = now();
		val r = ThreadLocalRandom.current();
		for (int n = 0; n < MAX; ){
			int i = r.nextInt(0, MAX);
			var e = kv.get(Long.toString(7900_000_00_00L + i));
			if (++n % 20_000 == 0) System.out.println(n);
			assertEquals(Long.toString(7900_000_00_00L + i).repeat(7), e.getValueAsString());
		}
		System.out.println(perfToString(t, now(), MAX));
	}
}