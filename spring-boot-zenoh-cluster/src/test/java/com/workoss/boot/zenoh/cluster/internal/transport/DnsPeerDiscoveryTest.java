package com.workoss.boot.zenoh.cluster.internal.transport;

import com.workoss.boot.zenoh.cluster.internal.transport.DnsPeerDiscovery.Settings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.naming.NameNotFoundException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
class DnsPeerDiscoveryTest {

    @Test
    void validatesEnabledSettingsAndAllowsDisabledDefaults() {
        assertFalse(new Settings(null, 0, 0, null, null, 0).enabled());
        assertFalse(new Settings(" \t ", -1, -1, "", "", -1).enabled());
        Settings settings = new Settings(" peers.ns.svc.cluster.local. ", 7447, 1000,
                " 10.0.0.1 ", null, 100);
        assertTrue(settings.enabled());
        assertEquals("peers.ns.svc.cluster.local.", settings.name());
        assertEquals("10.0.0.1", settings.selfAddress());
        for (int port : List.of(-1, 0, 65536)) {
            assertThrows(IllegalArgumentException.class,
                    () -> new Settings("peers", port, 1, null, null, 1));
        }
        for (long invalid : List.of(-1L, 0L)) {
            assertThrows(IllegalArgumentException.class,
                    () -> new Settings("peers", 1, invalid, null, null, 1));
            assertThrows(IllegalArgumentException.class,
                    () -> new Settings("peers", 65535, 1, null, null, invalid));
        }
        assertTrue(new Settings("peers", 65535, 1, "::1", "[::1]:5353", 1).enabled());
        assertTrue(new Settings("peers", 1, 1, "[2001:db8::1]", "2001:db8::53", 1).enabled());
    }

    @Test
    void rejectsHostnamesAndInvalidNumericAddressesWithoutDnsLookups() {
        for (String invalid : List.of("ldap://localhost/peers", "dns://localhost/peers", "a..b", "/peers",
                "bad name", "-peers", "peers-", "a".repeat(64))) {
            assertThrows(IllegalArgumentException.class,
                    () -> new Settings(invalid, 7447, 1, null, null, 1), invalid);
        }
        for (String invalid : List.of("must-not-resolve.invalid", "localhost", "127.1", "256.0.0.1",
                "10.0.0.-1", "10.0.0.1:7447", "[::1", "2001:db8::xyz", "1:2:3")) {
            assertThrows(IllegalArgumentException.class,
                    () -> new Settings("peers", 7447, 1, invalid, null, 1), invalid);
        }
        for (String invalid : List.of("localhost:53", "dns.invalid", "127.0.0.1:0",
                "127.0.0.1:65536", "127.0.0.1:", "[::1]:abc", "[::1]junk", "[::1")) {
            assertThrows(IllegalArgumentException.class,
                    () -> new Settings("peers", 7447, 1, null, invalid, 1), invalid);
        }
    }

    @Test
    void disabledDiscoveryNeverResolvesOrUpdates() {
        AtomicInteger calls = new AtomicInteger();
        try (DnsPeerDiscovery discovery = new DnsPeerDiscovery(
                new Settings("", 0, 0, null, null, 0), List.of(), endpoints -> calls.incrementAndGet(),
                "disabled", settings -> {
                    calls.incrementAndGet();
                    return List.of(ip("10.0.0.1"));
                })) {
            discovery.start();
            discovery.refresh();
            assertEquals(0, calls.get());
        }
    }

    @Test
    void mergesSortsAndDeduplicatesWithoutOverwritingStaticEndpoints() throws Exception {
        AtomicReference<List<InetAddress>> answer = new AtomicReference<>(List.of(
                ip("2001:db8::2"), ip("10.0.0.3"), ip("10.0.0.1"), ip("10.0.0.3"), ip("127.0.0.1")));
        List<List<String>> updates = new ArrayList<>();
        List<String> configured = new ArrayList<>(List.of("tcp/static.invalid:7447", "10.0.0.3:7447",
                "tcp/static.invalid:7447", "10.0.0.1:7447"));
        try (DnsPeerDiscovery discovery = new DnsPeerDiscovery(settings("10.0.0.1"), configured,
                updates::add, "merge", ignored -> answer.get())) {
            configured.clear();
            discovery.refresh();
            assertEquals(List.of(List.of("10.0.0.1:7447", "10.0.0.3:7447", "127.0.0.1:7447",
                    "[2001:db8:0:0:0:0:0:2]:7447", "tcp/static.invalid:7447")), updates);
            assertThrows(UnsupportedOperationException.class, () -> updates.getFirst().add("other"));
            answer.set(List.of(ip("127.0.0.1"), ip("10.0.0.3"), ip("2001:db8:0:0:0:0:0:2")));
            discovery.refresh();
            assertEquals(1, updates.size(), "Only endpoint set changes should update the transport");
            answer.set(List.of());
            discovery.refresh();
            assertEquals(List.of("10.0.0.1:7447", "10.0.0.3:7447", "tcp/static.invalid:7447"),
                    updates.getLast());
        }
    }

    @Test
    void excludesCanonicalIpv6SelfAddress() throws Exception {
        List<List<String>> updates = new ArrayList<>();
        try (DnsPeerDiscovery discovery = new DnsPeerDiscovery(settings("[2001:db8::1]"), List.of(),
                updates::add, "ipv6-self", ignored -> List.of(ip("2001:db8:0:0:0:0:0:1"), ip("2001:db8::2")))) {
            discovery.refresh();
            assertEquals(List.of(List.of("[2001:db8:0:0:0:0:0:2]:7447")), updates);
        }
    }

    @Test
    void automaticallyExcludesNetworkInterfaceAddresses() throws Exception {
        List<InetAddress> local = new ArrayList<>();
        for (NetworkInterface networkInterface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
            local.addAll(Collections.list(networkInterface.getInetAddresses()));
        }
        assertFalse(local.isEmpty());
        local.add(ip("192.0.2.219"));
        List<List<String>> updates = new ArrayList<>();
        try (DnsPeerDiscovery discovery = new DnsPeerDiscovery(settings(null), List.of(), updates::add,
                "local", ignored -> local)) {
            discovery.refresh();
            assertEquals(List.of(List.of("192.0.2.219:7447")), updates);
        }
    }

    @Test
    void failuresKeepAppliedEndpointsAndRetryWhileEmptySuccessClearsDynamicPeers() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        List<List<String>> updates = new ArrayList<>();
        try (DnsPeerDiscovery discovery = new DnsPeerDiscovery(settings("10.0.0.1"), List.of("static:7447"),
                updates::add, "retry", ignored -> switch (attempts.incrementAndGet()) {
                    case 1 -> List.of(ip("10.0.0.2"));
                    case 2 -> throw new NameNotFoundException("NXDOMAIN");
                    case 3 -> List.of(ip("10.0.0.3"));
                    default -> List.of();
                })) {
            discovery.refresh();
            discovery.refresh();
            assertEquals(List.of(List.of("10.0.0.2:7447", "static:7447")), updates);
            discovery.refresh();
            discovery.refresh();
            discovery.refresh();
            assertEquals(List.of(List.of("10.0.0.2:7447", "static:7447"),
                    List.of("10.0.0.3:7447", "static:7447"), List.of("static:7447")), updates);
        }
    }

    @Test
    void updaterFailureDoesNotMarkTheNewSetAsApplied() {
        AtomicInteger attempts = new AtomicInteger();
        List<List<String>> updates = new ArrayList<>();
        try (DnsPeerDiscovery discovery = new DnsPeerDiscovery(settings("10.0.0.1"), List.of(), endpoints -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("session rebuild failed");
            }
            updates.add(endpoints);
        }, "updater-retry", ignored -> List.of(ip("10.0.0.2")))) {
            discovery.refresh();
            discovery.refresh();
            discovery.refresh();
            assertEquals(2, attempts.get());
            assertEquals(List.of(List.of("10.0.0.2:7447")), updates);
        }
    }

    @Test
    void updaterFailureRetriesEvenWhenDnsRevertsToThePreviousSet() throws Exception {
        AtomicReference<List<InetAddress>> answer = new AtomicReference<>(List.of(ip("10.0.0.2")));
        AtomicInteger attempts = new AtomicInteger();
        List<List<String>> updates = new ArrayList<>();
        try (DnsPeerDiscovery discovery = new DnsPeerDiscovery(settings("10.0.0.1"), List.of(), endpoints -> {
            if (attempts.incrementAndGet() == 2) {
                throw new IllegalStateException("failed after closing native session");
            }
            updates.add(endpoints);
        }, "reverted-dns", ignored -> answer.get())) {
            discovery.refresh();
            answer.set(List.of(ip("10.0.0.3")));
            discovery.refresh();
            answer.set(List.of(ip("10.0.0.2")));
            discovery.refresh();
            assertEquals(3, attempts.get());
            assertEquals(List.of(List.of("10.0.0.2:7447"), List.of("10.0.0.2:7447")), updates);
        }
    }

    @Test
    void unchangedStaticOnlySetDoesNotTriggerAnUpdate() {
        AtomicInteger calls = new AtomicInteger();
        try (DnsPeerDiscovery discovery = new DnsPeerDiscovery(settings("10.0.0.1"),
                List.of("10.0.0.2:7447"), endpoints -> calls.incrementAndGet(), "static-only",
                ignored -> List.of(ip("10.0.0.2")))) {
            discovery.refresh();
            assertEquals(0, calls.get());
        }
    }

    @Test
    void startsImmediatelyOnOneIndependentDaemonAndRefreshesWithFixedDelay() throws Exception {
        BlockingQueue<List<String>> updates = new LinkedBlockingQueue<>();
        List<Thread> workers = new CopyOnWriteArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (DnsPeerDiscovery discovery = new DnsPeerDiscovery(
                new Settings("peers", 7447, 20, "10.0.0.1", null, 100), List.of(), updates::add,
                "scheduled", ignored -> {
                    workers.add(Thread.currentThread());
                    int call = calls.incrementAndGet();
                    if (call == 1) {
                        entered.countDown();
                        release.await();
                    }
                    return List.of(ip("10.0.0." + (call + 1)));
                })) {
            discovery.start();
            discovery.start();
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            assertTrue(workers.getFirst().isDaemon());
            assertEquals("zenoh-cluster-dns-scheduled", workers.getFirst().getName());
            assertNull(updates.poll(60, TimeUnit.MILLISECONDS));
            assertEquals(1, calls.get(), "A slow DNS resolution must not overlap another refresh");
            release.countDown();
            assertEquals(List.of("10.0.0.2:7447"), updates.poll(2, TimeUnit.SECONDS));
            assertEquals(List.of("10.0.0.3:7447"), updates.poll(2, TimeUnit.SECONDS));
            assertTrue(workers.stream().allMatch(worker -> worker == workers.getFirst()));
        } finally {
            release.countDown();
        }
    }

    @Test
    void closeDoesNotWaitForDnsAndPreventsAnInFlightResolutionFromUpdating() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger updates = new AtomicInteger();
        try (DnsPeerDiscovery discovery = new DnsPeerDiscovery(settings("10.0.0.1"), List.of(),
                endpoints -> updates.incrementAndGet(), "blocked-dns", ignored -> {
                    entered.countDown();
                    release.await();
                    return List.of(ip("10.0.0.2"));
                }); var worker = Executors.newSingleThreadExecutor()) {
            var refresh = worker.submit(discovery::refresh);
            try {
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                assertTimeoutPreemptively(Duration.ofMillis(500), discovery::close);
            } finally {
                release.countDown();
            }
            refresh.get(2, TimeUnit.SECONDS);
            discovery.start();
            discovery.refresh();
            assertEquals(0, updates.get());
        }
    }

    @Test
    void closeShutsDownAndInterruptsTheScheduledWorker() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        AtomicInteger updates = new AtomicInteger();
        try (DnsPeerDiscovery discovery = new DnsPeerDiscovery(settings("10.0.0.1"), List.of(),
                endpoints -> updates.incrementAndGet(), "shutdown", ignored -> {
                    entered.countDown();
                    try {
                        new CountDownLatch(1).await();
                    } catch (InterruptedException e) {
                        interrupted.countDown();
                        throw e;
                    }
                    return List.of();
                })) {
            discovery.start();
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            discovery.close();
            assertTrue(interrupted.await(2, TimeUnit.SECONDS));
            discovery.close();
            assertEquals(0, updates.get());
        }
    }

    @Test
    void closeWaitsForAnAlreadyRunningUpdaterAndPreventsLaterUpdates() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch closeEntered = new CountDownLatch(1);
        AtomicInteger updates = new AtomicInteger();
        try (DnsPeerDiscovery discovery = new DnsPeerDiscovery(settings("10.0.0.1"), List.of(), endpoints -> {
            updates.incrementAndGet();
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }, "blocked-updater", ignored -> List.of(ip("10.0.0.2"))); var workers = Executors.newFixedThreadPool(2)) {
            var refresh = workers.submit(discovery::refresh);
            try {
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                var close = workers.submit(() -> {
                    closeEntered.countDown();
                    discovery.close();
                });
                assertTrue(closeEntered.await(2, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
                release.countDown();
                close.get(2, TimeUnit.SECONDS);
                refresh.get(2, TimeUnit.SECONDS);
                discovery.refresh();
                assertEquals(1, updates.get());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void realJndiQueriesAAndAaaaAndSeesChangedRecordsDespitePositiveTtl() throws Exception {
        List<List<String>> updates = new ArrayList<>();
        try (UdpDnsServer dns = new UdpDnsServer();
             DnsPeerDiscovery discovery = new DnsPeerDiscovery(dns.settings(200), List.of("tcp/static:7447"),
                     updates::add, "real-dns")) {
            dns.answer.set(new Answer(List.of("10.0.0.1", "10.0.0.3", "10.0.0.3"),
                    List.of("2001:db8::3"), 0, 0, false));
            discovery.refresh();
            assertEquals(List.of(List.of("10.0.0.3:7447", "[2001:db8:0:0:0:0:0:3]:7447", "tcp/static:7447")), updates);
            dns.answer.set(new Answer(List.of("10.0.0.2"), List.of("2001:db8::2"), 0, 0, false));
            discovery.refresh();
            assertEquals(List.of("10.0.0.2:7447", "[2001:db8:0:0:0:0:0:2]:7447", "tcp/static:7447"),
                    updates.getLast());
            discovery.refresh();
            assertEquals(2, updates.size());
            assertEquals(List.of(1, 28, 1, 28, 1, 28), dns.queries.stream().map(Query::type).toList());
            assertTrue(dns.queries.stream().allMatch(query -> query.name().equals("peers.ns.svc.cluster.local")));
        }
    }

    @Test
    void realJndiSupportsAaaaOnlyAndClearsDynamicPeersOnNoerrorEmptyRecords() throws Exception {
        List<List<String>> updates = new ArrayList<>();
        try (UdpDnsServer dns = new UdpDnsServer();
             DnsPeerDiscovery discovery = new DnsPeerDiscovery(dns.settings(200), List.of("static:7447"),
                     updates::add, "empty-dns")) {
            dns.answer.set(new Answer(List.of(), List.of("2001:db8::5"), 0, 0, false));
            discovery.refresh();
            assertEquals(List.of(List.of("[2001:db8:0:0:0:0:0:5]:7447", "static:7447")), updates);
            dns.answer.set(Answer.empty());
            discovery.refresh();
            discovery.refresh();
            assertEquals(List.of(List.of("[2001:db8:0:0:0:0:0:5]:7447", "static:7447"),
                    List.of("static:7447")), updates);
        }
    }

    @Test
    void realJndiRetainsTheWholeSetForNxdomainServfailAndPartialQueryFailures() throws Exception {
        List<List<String>> updates = new ArrayList<>();
        try (UdpDnsServer dns = new UdpDnsServer();
             DnsPeerDiscovery discovery = new DnsPeerDiscovery(dns.settings(200), List.of(),
                     updates::add, "dns-errors")) {
            dns.answer.set(new Answer(List.of("10.0.0.2"), List.of(), 0, 0, false));
            discovery.refresh();
            for (Answer failure : List.of(
                    new Answer(List.of(), List.of(), 3, 3, false),
                    new Answer(List.of(), List.of(), 2, 2, false),
                    new Answer(List.of("10.0.0.9"), List.of(), 0, 2, false))) {
                dns.answer.set(failure);
                discovery.refresh();
                assertEquals(List.of(List.of("10.0.0.2:7447")), updates);
            }
            dns.answer.set(new Answer(List.of("10.0.0.4"), List.of(), 0, 0, false));
            discovery.refresh();
            assertEquals(List.of(List.of("10.0.0.2:7447"), List.of("10.0.0.4:7447")), updates);
        }
    }

    @Test
    void realJndiTimeoutIsBoundedAndTheNextRefreshCanRecover() throws Exception {
        List<List<String>> updates = new ArrayList<>();
        try (UdpDnsServer dns = new UdpDnsServer();
             DnsPeerDiscovery discovery = new DnsPeerDiscovery(dns.settings(80), List.of(),
                     updates::add, "dns-timeout")) {
            dns.answer.set(new Answer(List.of(), List.of(), 0, 0, true));
            assertTimeoutPreemptively(Duration.ofSeconds(2), discovery::refresh);
            assertTrue(updates.isEmpty());
            assertFalse(dns.queries.isEmpty());
            assertTrue(dns.queries.size() <= 2, "DNS retries must be bounded");
            dns.answer.set(new Answer(List.of("10.0.0.2"), List.of(), 0, 0, false));
            discovery.refresh();
            assertEquals(List.of(List.of("10.0.0.2:7447")), updates);
        }
    }

    @Test
    void realJndiAcceptsRelativeDnsNames() throws Exception {
        List<List<String>> updates = new ArrayList<>();
        try (UdpDnsServer dns = new UdpDnsServer();
             DnsPeerDiscovery discovery = new DnsPeerDiscovery(new Settings("peers", 7447, 1000,
                     "10.0.0.1", dns.server(), 200), List.of(), updates::add, "relative-dns")) {
            dns.answer.set(new Answer(List.of("10.0.0.2"), List.of(), 0, 0, false));
            discovery.refresh();
            assertEquals(List.of(List.of("10.0.0.2:7447")), updates);
            assertEquals(List.of(new Query("peers", 1), new Query("peers", 28)), dns.queries);
        }
    }

    private static Settings settings(String selfAddress) {
        return new Settings("peers.ns.svc.cluster.local.", 7447, 1000, selfAddress, null, 100);
    }

    private static InetAddress ip(String literal) throws Exception {
        return InetAddress.getByName(literal);
    }

    private record Query(String name, int type) { }

    record Answer(List<String> ipv4, List<String> ipv6, int aRcode, int aaaaRcode, boolean drop) {
        static Answer empty() {
            return new Answer(List.of(), List.of(), 0, 0, false);
        }
    }

    /** Real UDP DNS input for JNDI; answers have a positive TTL to detect accidental JVM caching. */
    static final class UdpDnsServer implements AutoCloseable {
        final AtomicReference<Answer> answer = new AtomicReference<>(Answer.empty());
        final List<Query> queries = new CopyOnWriteArrayList<>();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final DatagramSocket socket;
        private final Thread worker;

        UdpDnsServer() throws Exception {
            socket = new DatagramSocket(0, ip("127.0.0.1"));
            worker = new Thread(this::serve, "dns-test-fixture");
            worker.setDaemon(true);
            worker.start();
        }

        String server() {
            return "127.0.0.1:" + socket.getLocalPort();
        }

        Settings settings(long timeoutMillis) {
            return new Settings("peers.ns.svc.cluster.local.", 7447, 1000, "10.0.0.1", server(), timeoutMillis);
        }

        private void serve() {
            try {
                while (!socket.isClosed()) {
                    DatagramPacket packet = new DatagramPacket(new byte[4096], 4096);
                    socket.receive(packet);
                    byte[] request = Arrays.copyOf(packet.getData(), packet.getLength());
                    int cursor = 12;
                    List<String> labels = new ArrayList<>();
                    int length;
                    while ((length = Byte.toUnsignedInt(request[cursor++])) != 0) {
                        labels.add(new String(request, cursor, length, StandardCharsets.US_ASCII));
                        cursor += length;
                    }
                    int type = (Byte.toUnsignedInt(request[cursor]) << 8) | Byte.toUnsignedInt(request[cursor + 1]);
                    int questionEnd = cursor + 4;
                    queries.add(new Query(String.join(".", labels), type));
                    Answer current = answer.get();
                    if (current.drop()) {
                        continue;
                    }
                    int rcode = type == 28 ? current.aaaaRcode() : current.aRcode();
                    List<String> records = rcode != 0 ? List.of()
                            : type == 1 ? current.ipv4() : type == 28 ? current.ipv6() : List.of();
                    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                    try (DataOutputStream response = new DataOutputStream(bytes)) {
                        response.write(request, 0, 2); // Transaction ID.
                        response.writeShort(0x8180 | rcode); // QR, RD, RA, RCODE; no truncation.
                        response.writeShort(1);
                        response.writeShort(records.size());
                        response.writeShort(0);
                        response.writeShort(0);
                        response.write(request, 12, questionEnd - 12);
                        for (String record : records) {
                            byte[] address = ip(record).getAddress();
                            response.writeShort(0xc00c); // Compressed owner points at question name.
                            response.writeShort(type);
                            response.writeShort(1); // IN.
                            response.writeInt(600); // Positive TTL, changed on subsequent refreshes.
                            response.writeShort(address.length);
                            response.write(address);
                        }
                    }
                    byte[] response = bytes.toByteArray();
                    socket.send(new DatagramPacket(response, response.length, packet.getSocketAddress()));
                }
            } catch (SocketException e) {
                if (!socket.isClosed()) {
                    failure.set(e);
                }
            } catch (Throwable e) {
                failure.set(e);
            }
        }

        @Override
        public void close() throws Exception {
            socket.close();
            worker.join(2000);
            assertFalse(worker.isAlive(), "DNS fixture thread must terminate");
            assertNull(failure.get(), "DNS fixture failed: " + failure.get());
        }
    }
}
