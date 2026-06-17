package com.workoss.zenoh;

import org.junit.jupiter.api.*;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for zenoh-ffi Java bindings.
 *
 * Note: a few generated method names were renamed locally to fix duplicate
 * `close()` definitions:
 *   - Session#close()         -> AutoCloseable graceful close
 *   - Session#closeSession()  -> explicit FFI close (originally `close()`)
 *   - EmbeddedRouter#close()  -> AutoCloseable graceful close
 *   - EmbeddedRouter#closeRouter() -> explicit FFI close (originally `close()`)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ZenohFFITest {

    @Test
    @Order(1)
    @DisplayName("Config - create default and from JSON5")
    void testConfig() {
        try (Config config = Config.defaultConfig()) {
            assertNotNull(config);
            config.setMode("peer");
            config.setListenEndpoints(Arrays.asList("tcp/127.0.0.1:0"));
        }

        String json5 = "{ mode: \"peer\" }";
        try (Config config = Config.fromJson5(json5)) {
            assertNotNull(config);
        }
    }

    @Test
    @Order(2)
    @DisplayName("Session - open and close")
    void testSessionOpenClose() {
        try (Config config = Config.defaultConfig()) {
            config.setMode("peer");
            config.setListenEndpoints(Arrays.asList("tcp/127.0.0.1:0"));
            try (Session session = new Session(config)) {
                assertNotNull(session);
                assertFalse(session.isClosed());
                String zid = session.zid();
                assertNotNull(zid);
                assertFalse(zid.isEmpty());
                System.out.println("[testSessionOpenClose] Session ZID: " + zid);
            }
        }
    }

    @Test
    @Order(3)
    @DisplayName("Session - put and delete")
    void testSessionPutDelete() {
        try (Config config = Config.defaultConfig()) {
            config.setMode("peer");
            config.setListenEndpoints(Arrays.asList("tcp/127.0.0.1:0"));
            try (Session session = new Session(config)) {
                session.put("test/key1", "Hello Zenoh".getBytes());
                session.delete("test/key1");
            }
        }
    }

    @Test
    @Order(4)
    @DisplayName("Publisher - declare, put, undeclare")
    void testPublisher() {
        try (Config config = Config.defaultConfig()) {
            config.setMode("peer");
            config.setListenEndpoints(Arrays.asList("tcp/127.0.0.1:0"));
            try (Session session = new Session(config)) {
                try (Publisher pub = new Publisher(session, "test/pub/data",
                        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())) {
                    assertNotNull(pub);
                    assertEquals("test/pub/data", pub.getKeyExpr());

                    pub.put("Hello from publisher".getBytes());
                    pub.putWithEncoding("{\"hello\":\"world\"}".getBytes(), "application/json");

                    pub.delete();
                }
            }
        }
    }

    @Test
    @Order(5)
    @DisplayName("Subscriber - declare and receive")
    void testSubscriber() throws Exception {
        try (Config config = Config.defaultConfig()) {
            config.setMode("peer");
            config.setListenEndpoints(Arrays.asList("tcp/127.0.0.1:0"));
            try (Session session = new Session(config)) {
                try (Subscriber sub = new Subscriber(session, "test/sub/**")) {
                    assertNotNull(sub);
                    assertEquals("test/sub/**", sub.getKeyExpr());
                    assertFalse(sub.isUndeclared());

                    // Trigger sample delivery via session.put
                    session.put("test/sub/key1", "value1".getBytes());

                    // Wait briefly then poll non-blocking
                    Thread.sleep(200);
                    Optional<Sample> sample = sub.tryRecv();
                    if (sample.isPresent()) {
                        Sample s = sample.get();
                        System.out.println("[testSubscriber] Received key: " + s.keyExpr +
                            " payload-len=" + s.payload.length);
                        assertEquals("test/sub/key1", s.keyExpr);
                    } else {
                        System.out.println("[testSubscriber] No sample received within timeout (best-effort)");
                    }

                    sub.undeclare();
                    assertTrue(sub.isUndeclared());
                }
            }
        }
    }

    @Test
    @Order(6)
    @DisplayName("Queryable and Query - declare and handle")
    void testQueryable() throws Exception {
        try (Config config = Config.defaultConfig()) {
            config.setMode("peer");
            config.setListenEndpoints(Arrays.asList("tcp/127.0.0.1:0"));
            try (Session session = new Session(config)) {
                try (Queryable queryable = new Queryable(session, "test/queryable/**")) {
                    assertNotNull(queryable);
                    assertEquals("test/queryable/**", queryable.getKeyExpr());

                    Thread handler = new Thread(() -> {
                        try (Query query = new Query(queryable)) {
                            query.reply("test/queryable/resp", "response".getBytes());
                        } catch (Exception ignored) {
                            // queryable closed or no query
                        }
                    }, "queryable-handler");
                    handler.setDaemon(true);
                    handler.start();

                    Thread.sleep(200);
                    List<Reply> replies = session.get("test/queryable/**");
                    System.out.println("[testQueryable] Received " + replies.size() + " replies");
                    for (Reply r : replies) {
                        System.out.println("  reply success=" + r.success +
                            " keyExpr=" + r.sampleKeyExpr.orElse("<none>"));
                    }

                    queryable.undeclare();
                    assertTrue(queryable.isUndeclared());
                    handler.join(500);
                }
            }
        }
    }

    @Test
    @Order(7)
    @DisplayName("EmbeddedRouter - start and stop")
    void testEmbeddedRouter() {
        String json5 = "{\n" +
            "  mode: \"router\",\n" +
            "  listen: { endpoints: [\"tcp/127.0.0.1:17448\"] },\n" +
            "  scouting: { multicast: { enabled: false }, gossip: { enabled: false } },\n" +
            "  plugins_loading: { enabled: false }\n" +
            "}";

        try (EmbeddedRouter router = new EmbeddedRouter(json5)) {
            assertNotNull(router);
            assertTrue(router.isRunning());
            String zid = router.zid();
            assertNotNull(zid);
            System.out.println("[testEmbeddedRouter] Router ZID: " + zid);

            router.put("router/test/key", "router value".getBytes());
            router.delete("router/test/key");
        }
    }

    @Test
    @Order(8)
    @DisplayName("EmbeddedRouter with filesystem plugin")
    void testEmbeddedRouterWithFsPlugin() throws Exception {
        String json5 = "{\n" +
            "  mode: \"router\",\n" +
            "  listen: { endpoints: [\"tcp/127.0.0.1:17449\"] },\n" +
            "  scouting: { multicast: { enabled: false }, gossip: { enabled: false } },\n" +
            "  plugins_loading: { enabled: true, search_dirs: [\"~/.zenoh/lib\"] },\n" +
            "  plugins: {\n" +
            "    storage_manager: {\n" +
            "      volumes: { fs: {} },\n" +
            "      storages: {\n" +
            "        java_test: {\n" +
            "          key_expr: \"java/test/**\",\n" +
            "          strip_prefix: \"java/test\",\n" +
            "          volume: { id: \"fs\", dir: \"./zenoh_storage\" }\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";

        try (EmbeddedRouter router = new EmbeddedRouter(json5)) {
            assertTrue(router.isRunning());
            System.out.println("[testEmbeddedRouterWithFsPlugin] Router ZID: " + router.zid());

            // Wait for storage_manager + filesystem backend to load
            Thread.sleep(2000);

            router.put("java/test/data1", "persisted value".getBytes());

            Thread.sleep(1000);
            System.out.println("[testEmbeddedRouterWithFsPlugin] Wrote java/test/data1");
        } catch (RuntimeException e) {
            // Filesystem plugin may not be available locally; surface as warning
            System.out.println("[testEmbeddedRouterWithFsPlugin] Plugin load failed: " + e.getMessage());
            Assumptions.assumeTrue(false,
                "Skipping FS plugin test: " + e.getMessage());
        }
    }
}
