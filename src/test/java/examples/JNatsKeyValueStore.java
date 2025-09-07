package examples;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueStatus;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamInfo;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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

/// https://docs.nats.io/nats-concepts/jetstream/key-value-store
/// https://natsbyexample.com/examples/kv/intro/java
@Slf4j
@Testcontainers
class JNatsKeyValueStore {
	@Container
	private static final GenericContainer<?> natsContainer =
			new GenericContainer<>(DockerImageName.parse("nats:latest"))
				// Copy the config file from resources to a specific path in the container
				.withCopyFileToContainer(MountableFile.forClasspathResource("nats_server.conf"), "/etc/nats/nats_server.conf")
				.withCommand("-c /etc/nats/nats_server.conf -js")
				.withExposedPorts(4222);
				//.withCreateContainerCmdModifier(cmd -> cmd.withCmd("--jetstream").withCmd("-c /etc/nats/nats_server.conf"));

	private static Connection nc;

	@BeforeAll
	static void setup () throws Exception {
		String natsUrl = "nats://" + natsContainer.getHost() +':'+ natsContainer.getMappedPort(4222);

		Options options = new Options.Builder()
			.server(natsUrl)
// .server("nats://127.0.0.1") // local NATS
// .servers(array("nats://10.3.153.193", "nats://10.3.153.194", "nats://10.3.153.195")) // NATS cluster
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

	/// @see io.nats.client.impl.NatsKeyValue#_write
	@Test
	void benchmarkAsyncHack () throws IOException, JetStreamApiException, InterruptedException {
		System.out.println("3️⃣ KV put using PublishAsync");

		KeyValueManagement kvm = nc.keyValueManagement();

		KeyValueConfiguration kvc = KeyValueConfiguration.builder()
			.name("benchmarkAsync")// . нельзя ⇒ JetStream KV_JNatsKeyValueStore-benchmark
			.compression(true)
			.storageType(StorageType.File)
			.maxHistoryPerKey(1)
			.build();

		KeyValueStatus keyValueStatus = kvm.create(kvc);// Create bucket if it doesn't exist
		System.out.println(keyValueStatus);

		// Retrieve the Key Value context once the bucket is created:
		KeyValue kv = nc.keyValue("benchmarkAsync");

		val js = (JetStream) ReflectionTestUtils.getField(kv, "js");
		val jsm = (JetStreamManagement) ReflectionTestUtils.getField(kv, "jsm");

		StreamInfo info = jsm.getStreamInfo("KV_benchmarkAsync");
		System.out.println("getMaxMsgsPerSubject: " + info.getConfiguration().getMaxMsgsPerSubject());
		System.out.println(info);
		System.out.println(js.getStreamContext("KV_benchmarkAsync"));

		val futures = new ArrayList<CompletableFuture<PublishAck>>(MAX);

		long t = now();
		for (int i = 0; i < MAX; ){
			String key = "$KV.benchmarkAsync."+ (7900_000_00_00L + i);
			futures.add(js.publishAsync(key, Long.toString(7900_000_00_00L + i).repeat(7).getBytes(ISO_8859_1)));
			if (++i % 50_000 == 0) System.out.println(i);
		}
		System.out.println(perfToString(t, now(), MAX));
		futures.forEach(MagicUtils::await);
		System.out.println(perfToString(t, now(), MAX));
		futures.forEach(f->{
			assertFalse(MagicUtils.get(f).isDuplicate());
		});

		assertEquals(MAX,  futures.size());

		List<String> keys = kv.keys();
		assertEquals(MAX,  keys.size());
		keys.sort(null);
		for (int i = 0; i < MAX; i++){
			assertEquals(Long.toString(7900_000_00_00L + i), keys.get(i));
		}
		val w = new CountDownLatch(10);
		val counter = new AtomicInteger();
		System.out.println("4️⃣ Multi thread random reads");
		t = now();
		loop(10, ()->execute(()->{
				var r = ThreadLocalRandom.current();
				try {
					for (int n = 0; n < MAX; ){
						int i = r.nextInt(0, MAX);
						var key = Long.toString(7900_000_00_00L + i);
						var e = kv.get(key);
						if (++n % 20_000 == 0) System.out.println(n);
						if (e == null){
							System.err.println(n);
							System.err.println(key);
						} else {
							counter.incrementAndGet();
						}
						assertEquals(Long.toString(7900_000_00_00L + i).repeat(7), e.getValueAsString(), key);
					}
				} catch (Exception e){
					e.printStackTrace();
				}
				w.countDown();
			}));
		boolean done = w.await(999, TimeUnit.SECONDS);
		assertTrue(done);
		System.out.println(perfToString(t, now(), MAX*10));
		assertEquals(MAX*10, counter.get());

// https://github.com/nats-io/nats.go/discussions/1507#discussioncomment-14306747
// надо разрешить в JetStream только 1 элемент ⇒ это и будет value же...
//		System.out.println("А теперь скорость чтения...");
//		t = now();
//		for (int i = 0; i < 500_000; i++){
//			var e = kv.get(Long.toString(7900_000_00_00L + i));
//			if (i % 10_000 == 9_999) System.out.println(i);
//			assertEquals(Long.toString(7900_000_00_00L + i).repeat(7), e.getValueAsString());
//		}
//		System.out.println(MILLI.toString(t, now(), 500_000));
	}
}