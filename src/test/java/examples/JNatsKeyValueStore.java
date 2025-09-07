package examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.CompressionOption;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueStatus;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static examples.MagicUtils.close;
import static examples.MagicUtils.execute;
import static examples.MagicUtils.loop;
import static examples.MagicUtils.now;
import static examples.MagicUtils.perfToString;
import static java.nio.charset.StandardCharsets.*;
import static org.junit.jupiter.api.Assertions.*;

/// https://jira.platform-s.com/browse/PLAT-3225
/// https://cf.platform-s.com/pages/viewpage.action?pageId=137790743
/// https://docs.nats.io/nats-concepts/jetstream/key-value-store
/// https://natsbyexample.com/examples/kv/intro/java
@Slf4j
@Testcontainers
class JNatsKeyValueStore {
	@Container
	private static final GenericContainer<?> natsContainer =
			new GenericContainer<>(DockerImageName.parse("nats:latest"))
					.withExposedPorts(4222)
					.withCreateContainerCmdModifier(cmd -> cmd.withCmd("--jetstream"));

	private static Connection nc;

	@BeforeAll
	static void setup () throws Exception {
		String natsUrl = "nats://" + natsContainer.getHost() +':'+ natsContainer.getMappedPort(4222);

		Options options = new Options.Builder()
			.server(natsUrl)
//			.server("nats://127.0.0.1") // локально запущенный NATS
			//.servers(array("nats://10.3.153.193", "nats://10.3.153.194", "nats://10.3.153.195"))
			.connectionTimeout(Duration.ofSeconds(30))
			.build();

		nc = Nats.connectReconnectOnConnect(options);
	}

	@AfterAll
	static void tearDown () {
		close(nc);
	}

	@Test
	void basicKeyValue () throws IOException, JetStreamApiException {
		System.out.println("===== com.devinotele.common.nats.component.JNatsKeyValueStore#basicKeyValue =====");

		KeyValueManagement kvm = nc.keyValueManagement();

		KeyValueConfiguration kvc = KeyValueConfiguration.builder()
			.name("basicKeyValue")
			.compression(true)
			.storageType(StorageType.File)
			.build();

		KeyValueStatus keyValueStatus = kvm.create(kvc);
		System.out.println(keyValueStatus);

		// Retrieve the Key Value context once the bucket is created:
		KeyValue kv = nc.keyValue("basicKeyValue");

		kv.put("xxx.1", "OneAsUTF-8String👈");

		KeyValueEntry entry = kv.get("xxx.1");
		System.out.printf("%s %d -> %s\n", entry.getKey(), entry.getRevision(), entry.getValueAsString());
	}

	@Test
	void benchmark () throws IOException, JetStreamApiException {
		System.out.println("===== com.devinotele.common.nats.component.JNatsKeyValueStore#benchmark =====");

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

		kv.put("xxx.1", "OneAsUTF-8String👈");

		KeyValueEntry entry = kv.get("xxx.1");
		System.out.printf("%s %d -> %s\n", entry.getKey(), entry.getRevision(), entry.getValueAsString());

		//1. Создадим 100 млн ключей
		long t = now();
		for (int i = 0; i < 500_000; i++){
			kv.put(Long.toString(7900_000_00_00L + i), Long.toString(7900_000_00_00L + i).repeat(7));
			if (i % 10_000 == 9_999){
				System.out.println(i);
			}
		}
		System.out.println(perfToString(t, now(), 500_000));

		System.out.println("А теперь скорость чтения...");
		t = now();
		for (int i = 0; i < 500_000; i++){
			var e = kv.get(Long.toString(7900_000_00_00L + i));
			if (i % 10_000 == 9_999){
				System.out.println(i);
			}
			assertEquals(Long.toString(7900_000_00_00L + i).repeat(7), e.getValueAsString());
		}
		System.out.println(perfToString(t, now(), 500_000));
	}

	/// @see io.nats.client.impl.NatsKeyValue#_write
	@Test
	void benchmarkAsync () throws IOException, JetStreamApiException, InterruptedException {
		System.out.println("===== com.devinotele.common.nats.component.JNatsKeyValueStore#benchmark =====");

		var js = nc.jetStream();
		var jsm = nc.jetStreamManagement();
		jsm.addStream(StreamConfiguration.builder()
			.name("KV_benchmarkAsync")
			.subjects("$KV.benchmarkAsync.>")
			.compressionOption(CompressionOption.S2)
			.storageType(StorageType.File)
			.build());


		//1. Создадим 100 млн ключей
		long t = now();
		for (int i = 0; i < 500_000; ){
			String key = "$KV.benchmarkAsync."+ (7900_000_00_00L + i);
			js.publishAsync(key, Long.toString(7900_000_00_00L + i).repeat(7).getBytes(ISO_8859_1));
			if (++i % 10_000 == 0)
					System.out.println(i);
		}
		System.out.println(perfToString(t, now(), 500_000));

		val w = new CountDownLatch(10);
		val counter = new AtomicInteger();
		t = now();
		loop(10, ()->execute(()->{
				for (int n = 0; n < 500_000; ){
					int i = ThreadLocalRandom.current().nextInt(0, 500_000);
					String key = "$KV.benchmarkAsync." + ( 7900_000_00_00L + i );
					js.publishAsync(key, Long.toString(7900_000_00_00L + i).repeat(7).getBytes(ISO_8859_1));
					counter.incrementAndGet();
					if (++n % 10_000 == 0)
							System.out.println(n);
				}
				w.countDown();
			}));
		boolean done = w.await(999, TimeUnit.SECONDS);
		assertTrue(done);
		System.out.println(perfToString(t, now(), 5_000_000));
		assertEquals(5_000_000, counter.get());

// https://github.com/nats-io/nats.go/discussions/1507#discussioncomment-14306747
// надо разрешить в JetStream только 1 элемент ⇒ это и будет value же...
//		System.out.println("А теперь скорость чтения...");
//		t = now();
//		for (int i = 0; i < 500_000; i++){
//			var e = kv.get(Long.toString(7900_000_00_00L + i));
//			if (i % 10_000 == 9_999){
//				System.out.println(i);
//			}
//			assertEquals(Long.toString(7900_000_00_00L + i).repeat(7), e.getValueAsString());
//		}
//		System.out.println(MILLI.toString(t, now(), 500_000));
	}
}