/*
 * Copyright 2021 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.buffer.api.tests;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.BufferAllocator;
import io.netty.buffer.api.LeakInfo;
import io.netty.buffer.api.MemoryManager;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.management.Notification;
import javax.management.NotificationBroadcaster;
import javax.management.NotificationListener;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

public class BufferLeakDetectionTest extends BufferTestSupport {
    @ParameterizedTest
    @MethodSource("allocators")
    public void bufferMustNotLeakWhenClosedProperly(Fixture fixture) throws Exception {
        Object hint = new Object();
        AtomicInteger counter = new AtomicInteger();
        Semaphore gcEvents = new Semaphore(0);
        Consumer<LeakInfo> callback = forHint(hint, leak -> counter.incrementAndGet());
        try (var ignore1 = MemoryManager.onLeakDetected(callback);
             var ignore2 = installGcEventListener(() -> gcEvents.release());
             BufferAllocator allocator = fixture.createAllocator()) {
            var runnable = new CreateAndCloseBuffers(allocator, hint);
            var thread = new Thread(runnable);
            thread.start();
            gcEvents.acquire(); // Wait for a GC event to happen.
            thread.interrupt();
            thread.join();
            assertThat(counter.get()).isZero();
        }
    }

    @ParameterizedTest
    @MethodSource("allocators")
    public void bufferLeakMustBeDetectedWhenNotClosedProperty(Fixture fixture) throws Exception {
        Object hint = new Object();
        LinkedBlockingQueue<LeakInfo> leakQueue = new LinkedBlockingQueue<>();
        Consumer<LeakInfo> callback = forHint(hint, leak -> leakQueue.offer(leak));
        CreateAndLeakBuffers runnable;
        Thread thread;
        LeakInfo leakInfo;
        try (var ignore1 = MemoryManager.onLeakDetected(callback);
             var ignore2 = installGcEventListener(() -> {});
             BufferAllocator allocator = fixture.createAllocator()) {
            runnable = new CreateAndLeakBuffers(allocator, hint);
            thread = new Thread(runnable);
            thread.start();
            leakInfo = leakQueue.poll(10, TimeUnit.MINUTES);
            thread.interrupt();
        }
        thread.join();
        assertThat(leakInfo).isNotNull();
    }
    // todo buffer leak must be detected when not closed properly
    // todo buffer must not leak when closed after send
    // todo buffer leak must be detected when not closed after send

    private static AutoCloseable installGcEventListener(Runnable callback) {
        CallbackListener listener = new CallbackListener(callback);
        List<GarbageCollectorMXBean> garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean bean : garbageCollectorMXBeans) {
            if (bean instanceof NotificationBroadcaster) {
                NotificationBroadcaster broadcaster = (NotificationBroadcaster) bean;
                listener.install(broadcaster);
            }
        }
        return listener;
    }

    private static Consumer<LeakInfo> forHint(Object hint, Consumer<LeakInfo> consumer) {
        return leak -> {
            if (leak.stream().anyMatch(tracePoint -> tracePoint.hint() == hint)) {
                consumer.accept(leak);
            }
        };
    }

    private static void produceGarbage() {
        AtomicBoolean trigger = new AtomicBoolean();
        try (AutoCloseable ignore = installGcEventListener(() -> trigger.set(true))) {
            while (!trigger.get()) {
                //noinspection ResultOfMethodCallIgnored
                Arrays.stream(new int[1024]).mapToObj(String::valueOf).count();
            }
        } catch (Exception ignore) {
        }
    }

    private static class CreateAndCloseBuffers implements Runnable {
        private final BufferAllocator allocator;
        private final Object hint;

        CreateAndCloseBuffers(BufferAllocator allocator, Object hint) {
            this.allocator = allocator;
            this.hint = hint;
        }

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                try (Buffer buffer = allocator.allocate(128)) {
                    buffer.touch(hint);
                }
                produceGarbage();
            }
        }
    }

    private static class CreateAndLeakBuffers implements Runnable {
        private final BufferAllocator allocator;
        private final Object hint;

        CreateAndLeakBuffers(BufferAllocator allocator, Object hint) {
            this.allocator = allocator;
            this.hint = hint;
        }

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                Buffer buffer = allocator.allocate(128);
                buffer.touch(hint);
                produceGarbage();
            }
        }
    }

    private static class CallbackListener implements NotificationListener, AutoCloseable {
        private final Runnable callback;
        private final List<NotificationBroadcaster> installedBroadcasters;
        private volatile Notification n;

        CallbackListener(Runnable callback) {
            this.callback = callback;
            installedBroadcasters = new ArrayList<>();
        }

        public void install(NotificationBroadcaster broadcaster) {
            broadcaster.addNotificationListener(this, null, null);
            installedBroadcasters.add(broadcaster);
        }

        @Override
        public void handleNotification(Notification notification, Object handback) {
            n = notification;
            callback.run();
        }

        @Override
        public void close() throws Exception {
            for (NotificationBroadcaster broadcaster : installedBroadcasters) {
                broadcaster.removeNotificationListener(this);
            }
            System.out.println("n = " + n.getUserData());
        }
    }
}
