/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.reconciler.caching;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.kubernetes.operator.reconciler.caching.RunnableSupport.withErrorHandler;
import static org.apache.flink.kubernetes.operator.reconciler.caching.SyncOrAsyncExecutor.ExecutionState.COMPUTED_IN_TIME;
import static org.apache.flink.kubernetes.operator.reconciler.caching.SyncOrAsyncExecutor.ExecutionState.FAILED_IN_TIME;
import static org.apache.flink.kubernetes.operator.reconciler.caching.SyncOrAsyncExecutor.ExecutionState.GRACE_PERIOD_OVER;
import static org.apache.flink.kubernetes.operator.reconciler.caching.SyncOrAsyncExecutor.ExecutionState.INITIALIZED;

/**
 * An executor, that tries to execute an operation within a grace period. If the operation returns
 * within the grace period, the result is synchronously returned.
 *
 * <p>If the grace period elapses without the operation returning, it keeps executing the operation
 * in the background while returns with an empty result synchronously.
 *
 * <p>The results of the async operation could be processed by callbacks.
 */
public class SyncOrAsyncExecutor {

    private static final ExecutorService es = Executors.newCachedThreadPool();

    /** INITIALIZED is the start state, all the other states are end states. */
    enum ExecutionState {
        INITIALIZED,
        COMPUTED_IN_TIME,
        FAILED_IN_TIME,
        GRACE_PERIOD_OVER
    }

    @Data
    @AllArgsConstructor
    private static class SharedStore<T> {
        public final ExecutionState executionState;
        public final Optional<T> result;
        public final Optional<Exception> failure;

        public SharedStore<T> withExecutionState(ExecutionState executionState) {
            return new SharedStore<>(executionState, result, failure);
        }

        public SharedStore<T> withResult(T result) {
            // TODO: The value returned from Supplier.get will be stored in the result field,
            // although Optional.of does not support storing null values
            return new SharedStore<>(executionState, Optional.of(result), failure);
        }

        public SharedStore<T> withFailure(Exception e) {
            return new SharedStore<>(executionState, result, Optional.of(e));
        }

        public static <T> SharedStore<T> init() {
            return new SharedStore<>(INITIALIZED, Optional.empty(), Optional.empty());
        }
    }

    public static <T> Optional<T> runSyncOrAsync(
            Supplier<T> operation,
            long gracePeriodMillis,
            Consumer<T> successCallback,
            Consumer<Exception> failureCallback) {

        AtomicReference<SharedStore<T>> atomicStore = new AtomicReference<>(SharedStore.init());

        CountDownLatch latch = new CountDownLatch(1);

        // TODO: refactor to minimise code nesting
        es.execute(
                withErrorHandler(
                        () -> {
                            try {
                                T result = operation.get();
                                atomicStore.updateAndGet(
                                        st -> {
                                            switch (st.executionState) {
                                                case INITIALIZED:
                                                    return st.withResult(result)
                                                            .withExecutionState(COMPUTED_IN_TIME);
                                                case GRACE_PERIOD_OVER:
                                                    successCallback.accept(result);
                                                    return st;
                                                default:
                                                    throw exceptionWithUnexpectedState(
                                                            st.executionState);
                                            }
                                        });
                            } catch (Exception e) {
                                atomicStore.updateAndGet(
                                        st -> {
                                            switch (st.executionState) {
                                                case INITIALIZED:
                                                    return st.withFailure(e)
                                                            .withExecutionState(FAILED_IN_TIME);
                                                case GRACE_PERIOD_OVER:
                                                    failureCallback.accept(e);
                                                    return st;
                                                default:
                                                    throw exceptionWithUnexpectedState(
                                                            st.executionState);
                                            }
                                        });
                            } finally {
                                latch.countDown();
                            }
                        }));
        es.execute(
                withErrorHandler(
                        () -> {
                            Thread.sleep(gracePeriodMillis);
                            atomicStore.updateAndGet(
                                    st -> {
                                        switch (st.executionState) {
                                            case INITIALIZED:
                                                return st.withExecutionState(GRACE_PERIOD_OVER);
                                            case COMPUTED_IN_TIME:
                                            case FAILED_IN_TIME:
                                                return st; // nothing to do
                                            default:
                                                throw exceptionWithUnexpectedState(
                                                        st.executionState);
                                        }
                                    });
                            latch.countDown();
                        }));
        await(latch);

        SharedStore<T> store = atomicStore.get();
        switch (store.executionState) {
            case COMPUTED_IN_TIME:
                return Optional.of(store.result.get());
            case FAILED_IN_TIME:
                throw new RuntimeException(store.failure.get());
            case GRACE_PERIOD_OVER:
                return Optional.empty();
            default:
                throw exceptionWithUnexpectedState(store.executionState);
        }
    }

    private static RuntimeException exceptionWithUnexpectedState(ExecutionState executionState) {
        return new RuntimeException(
                String.format("Unexpected execution state: %s", executionState));
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
