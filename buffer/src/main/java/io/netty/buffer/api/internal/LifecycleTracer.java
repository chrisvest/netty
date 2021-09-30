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
package io.netty.buffer.api.internal;

import io.netty.buffer.api.Drop;
import io.netty.buffer.api.LeakInfo;
import io.netty.buffer.api.LeakInfo.TracePoint;
import io.netty.buffer.api.Owned;
import io.netty.buffer.api.Resource;
import io.netty.util.internal.UnstableApi;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Instances of this class record life cycle events of resources, to help debug life-cycle errors.
 */
@UnstableApi
public abstract class LifecycleTracer {
    static volatile int lifecycleTracingEnabled =
            System.getProperty("io.netty.buffer.lifecycleTracingEnabled") == null ? 0 : 1;

    /**
     * Get a tracer for a newly allocated resource.
     *
     * @return A new tracer for a resource.
     */
    public static LifecycleTracer get() {
        if (lifecycleTracingEnabled == 0 && LeakDetection.leakDetectionEnabled == 0) {
            return NoOpTracer.INSTANCE;
        }
        StackTracer stackTracer = new StackTracer();
        stackTracer.addTrace(StackTracer.WALKER.walk(new Trace(TraceType.ALLOCATE, 0)));
        return stackTracer;
    }

    /**
     * Add to the trace log that the object has been acquired, in other words the reference count has been incremented.
     *
     * @param acquires The new current number of acquires on the traced object.
     */
    public abstract void acquire(int acquires);

    /**
     * Add to the trace log that the object has been dropped.
     *
     * @param acquires The new current number of acquires on the traced object.
     */
    public abstract void drop(int acquires);

    /**
     * Add to the trace log that the object has been closed, in other words, the reference count has been decremented.
     *
     * @param acquires The new current number of acquires on the traced object.
     */
    public abstract void close(int acquires);

    /**
     * Add the hint object to the trace log.
     * The hint objects can be inspected later if a lifecycle related exception is thrown, or if the object leaks.
     *
     * @param acquires The current number of acquires on the traced object.
     * @param hint The hint object to attach to the trace log.
     */
    public abstract void touch(int acquires, Object hint);

    /**
     * Add to the trace log that the object is being sent.
     *
     * @param instance The owned instance being sent.
     * @param acquires The current number of acquires on this object.
     * @param <I> The resource interface for the object.
     * @param <T> The concrete type of the object.
     * @return An {@link Owned} instance that may trace the reception of the object.
     */
    public abstract <I extends Resource<I>, T extends ResourceSupport<I, T>> Owned<T> send(
            Owned<T> instance, int acquires);

    public abstract void splitTo(LifecycleTracer splitTracer, int acquires, int splitAcquires);

    protected abstract void receiveFrom(Trace sendTrace, int acquires);

    /**
     * Attach a life cycle trace log to the given exception.
     *
     * @param throwable The exception that will receive the trace log in the form of
     * {@linkplain Throwable#addSuppressed(Throwable) suppressed exceptions}.
     * @param <E> The concrete exception type.
     * @return The same exception instance, that can then be thrown.
     */
    public abstract <E extends Throwable> E attachTrace(E throwable);

    /**
     * Return the life-cycle trace as a {@link Collection} of {@link TracePoint}s.
     * The returned collection is not modifiable.
     *
     * @return A collection of trace points.
     */
    public abstract Collection<TracePoint> collectTraces();

    private static final class NoOpTracer extends LifecycleTracer {
        private static final NoOpTracer INSTANCE = new NoOpTracer();

        @Override
        public void acquire(int acquires) {
        }

        @Override
        public void drop(int acquires) {
        }

        @Override
        public void close(int acquires) {
        }

        @Override
        public void touch(int acquires, Object hint) {
        }

        @Override
        public <I extends Resource<I>, T extends ResourceSupport<I, T>> Owned<T> send(Owned<T> instance, int acquires) {
            return instance;
        }

        @Override
        public void splitTo(LifecycleTracer splitTracer, int acquires, int splitAcquires) {
        }

        @Override
        protected void receiveFrom(Trace sendTrace, int acquires) {
        }

        @Override
        public <E extends Throwable> E attachTrace(E throwable) {
            return throwable;
        }

        @Override
        public Collection<TracePoint> collectTraces() {
            return Collections.emptyList();
        }
    }

    private static final class StackTracer extends LifecycleTracer {
        private static final int MAX_TRACE_POINTS = Math.min(Integer.getInteger(
                "io.netty.buffer.api.internal.LifecycleTracer.MAX_TRACE_POINTS", 50), 1000);
        private static final StackWalker WALKER;
        static {
            int depth = Trace.TRACE_LIFECYCLE_DEPTH;
            WALKER = depth > 0 ? StackWalker.getInstance(Set.of(), depth + 2) : null;
        }

        private final ArrayDeque<Trace> traces = new ArrayDeque<>();
        private boolean dropped;

        @Override
        public void acquire(int acquires) {
            addTrace(WALKER.walk(new Trace(TraceType.ACQUIRE, acquires)));
        }

        void addTrace(Trace trace) {
            synchronized (traces) {
                if (traces.size() == MAX_TRACE_POINTS) {
                    traces.pollFirst();
                }
                Trace prev = traces.peekLast();
                if (prev != null) {
                    prev.next = trace;
                }
                traces.addLast(trace);
            }
        }

        @Override
        public void drop(int acquires) {
            dropped = true;
            addTrace(WALKER.walk(new Trace(TraceType.DROP, acquires)));
        }

        @Override
        public void close(int acquires) {
            if (!dropped) {
                addTrace(WALKER.walk(new Trace(TraceType.CLOSE, acquires)));
            }
        }

        @Override
        public void touch(int acquires, Object hint) {
            Trace trace = new Trace(TraceType.TOUCH, acquires);
            trace.attachmentType = AttachmentType.HINT;
            trace.attachment = hint;
            addTrace(WALKER.walk(trace));
        }

        @Override
        public <I extends Resource<I>, T extends ResourceSupport<I, T>> Owned<T> send(Owned<T> instance, int acquires) {
            Trace sendTrace = new Trace(TraceType.SEND, acquires);
            sendTrace.attachmentType = AttachmentType.RECEIVED_AT;
            addTrace(WALKER.walk(sendTrace));
            return new Owned<T>() {
                @Override
                public T transferOwnership(Drop<T> drop) {
                    T obj = instance.transferOwnership(drop);
                    LifecycleTracer tracer = ResourceSupport.getTracer(obj);
                    tracer.receiveFrom(sendTrace, ResourceSupport.getAcquires(obj));
                    return obj;
                }
            };
        }

        @Override
        protected void receiveFrom(Trace sendTrace, int acquires) {
            Trace receiveTrace = new Trace(TraceType.RECEIVE, acquires);
            receiveTrace.attachment = sendTrace;
            receiveTrace.attachmentType = AttachmentType.SEND_FROM;
            addTrace(receiveTrace);
            sendTrace.attachment = receiveTrace;
        }

        @Override
        public void splitTo(LifecycleTracer splitTracer, int acquires, int splitAcquires) {
            Trace splitOrigin = new Trace(TraceType.SPLIT, acquires);
            Trace splitOff = new Trace(TraceType.SPLIT, splitAcquires);
            splitOrigin.attachmentType = AttachmentType.SPLIT_TO;
            splitOrigin.attachment = splitOff;
            splitOff.attachmentType = AttachmentType.SPLIT_FROM;
            splitOff.attachment = splitOrigin;
            addTrace(WALKER.walk(splitOrigin));
            if (splitTracer instanceof StackTracer) {
                StackTracer tracer = (StackTracer) splitTracer;
                tracer.addTrace(WALKER.walk(splitOff));
            }
        }

        @Override
        public <E extends Throwable> E attachTrace(E throwable) {
            synchronized (traces) {
                long timestamp = System.nanoTime();
                for (Trace trace : traces) {
                    trace.attach(throwable, timestamp);
                }
            }
            return throwable;
        }

        @Override
        public Collection<TracePoint> collectTraces() {
            return Collections.unmodifiableCollection(Collections.synchronizedCollection(traces));
        }
    }

    static final class Trace implements Function<Stream<StackWalker.StackFrame>, Trace>, LeakInfo.TracePoint {
        private static final int TRACE_LIFECYCLE_DEPTH;
        static {
            int traceDefault = 50;
            TRACE_LIFECYCLE_DEPTH = Math.max(Integer.getInteger(
                    "io.netty.buffer.api.internal.LifecycleTracer.TRACE_LIFECYCLE_DEPTH", traceDefault), 0);
        }

        final TraceType type;
        final int acquires;
        final long timestamp;
        volatile AttachmentType attachmentType;
        volatile Object attachment;
        volatile StackWalker.StackFrame[] frames;
        volatile Trace next;

        Trace(TraceType type, int acquires) {
            this.type = type;
            this.acquires = acquires;
            timestamp = System.nanoTime();
        }

        @Override
        public Trace apply(Stream<StackWalker.StackFrame> frames) {
            this.frames = frames.limit(TRACE_LIFECYCLE_DEPTH + 1).toArray(StackWalker.StackFrame[]::new);
            return this;
        }

        public <E extends Throwable> void attach(E throwable, long timestamp) {
            Traceback exception = getTraceback(timestamp, true);
            throwable.addSuppressed(exception);
        }

        @Override
        public Throwable getTraceback() {
            try {
                return getTraceback(System.nanoTime(), true);
            } catch (Exception e) {
                return e;
            }
        }

        private Traceback getTraceback(long timestamp, boolean recurse) {
            String message = type.name();
            Trace associatedTrace = getAssociatedTrace();
            message = explainAttachment(message, associatedTrace);
            message += " (current acquires = " + acquires + ") T" + (this.timestamp - timestamp) / 1000 + "µs.";
            Traceback exception = new Traceback(message);
            StackTraceElement[] stackTrace = framesToStackTrace();
            exception.setStackTrace(stackTrace);
            if (associatedTrace != null && recurse) {
                exception.addSuppressed(associatedTrace.getTraceback(timestamp, false));
                associatedTrace = associatedTrace.next;
                while (associatedTrace != null) {
                    exception.addSuppressed(associatedTrace.getTraceback(timestamp, recurse));
                    associatedTrace = associatedTrace.next;
                }
            }
            return exception;
        }

        private Trace getAssociatedTrace() {
            Object associated = attachment;
            if (associated instanceof Trace) {
                return (Trace) associated;
            }
            return null;
        }

        private String explainAttachment(String message, Trace associatedTrace) {
            AttachmentType type = attachmentType;
            if (type == null) {
                return message;
            }
            switch (type) {
            case RECEIVED_AT:
                if (associatedTrace == null) {
                    message += " (sent but not received)";
                } else {
                    message += " (sent and received)";
                }
                break;
            case SEND_FROM:
                message += " (from a send)";
                break;
            case SPLIT_TO:
                message += " (split into two)";
                break;
            case SPLIT_FROM:
                message += " (split from other object)";
                break;
            }
            return message;
        }

        private StackTraceElement[] framesToStackTrace() {
            StackTraceElement[] stackTrace = new StackTraceElement[frames.length];
            for (int i = 0; i < frames.length; i++) {
                stackTrace[i] = frames[i].toStackTraceElement();
            }
            return stackTrace;
        }

        @Override
        public Object hint() {
            return attachmentType == AttachmentType.HINT? attachment : null;
        }
    }

    private static final class Traceback extends Throwable {
        private static final long serialVersionUID = 941453986194634605L;

        Traceback(String message) {
            super(message);
        }

        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }

    private enum TraceType {
        ALLOCATE,
        ACQUIRE,
        CLOSE,
        DROP,
        SEND,
        RECEIVE,
        TOUCH,
        SPLIT,
    }

    private enum AttachmentType {
        /**
         * Tracer of sending object.
         */
        SEND_FROM,
        /**
         * Tracer of object that was received, after being sent.
         */
        RECEIVED_AT,
        /**
         * Tracer of origin object of a split.
         */
        SPLIT_FROM,
        /**
         * Tracer of object split from this traced object.
         */
        SPLIT_TO,
        /**
         * Object is a hint from a {@link Resource#touch(Object)} call.
         */
        HINT,
    }
}
