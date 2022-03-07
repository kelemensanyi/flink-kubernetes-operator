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

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Cache for storing reconciliation results. */
public class Cache<R> {
    // TODO: implement cache invalidation

    private final ConcurrentHashMap<ResourceID, CacheEntry<R>> store = new ConcurrentHashMap<>();

    public CacheEntry<R> get(ResourceID id) {
        return store.computeIfAbsent(id, id0 -> CacheEntry.emptyEntryFor(id));
    }

    public CacheEntry<R> put(ResourceID id, CacheEntry<R> entry) {
        return store.put(id, entry);
    }

    /** An entry in the cache. */
    @Data
    @AllArgsConstructor
    public static final class CacheEntry<R> {
        public final ResourceID id;
        public final Optional<R> value; // TODO: that value is not really used for anything
        public final Optional<ReconcileResult> pendingResult;
        public final boolean asyncRunning;

        public CacheEntry<R> withValue(R value) {
            return new CacheEntry(id, Optional.of(value), pendingResult, asyncRunning);
        }

        public CacheEntry<R> withPendingResult(ReconcileResult pendingResult) {
            return new CacheEntry(id, value, Optional.of(pendingResult), asyncRunning);
        }

        public CacheEntry<R> withAsyncRunning(boolean asyncRunning) {
            return new CacheEntry(id, value, pendingResult, asyncRunning);
        }

        public CacheEntry<R> withEmptyPendingResult() {
            return new CacheEntry(id, value, Optional.empty(), asyncRunning);
        }

        public static <R> CacheEntry<R> emptyEntryFor(ResourceID id) {
            return new CacheEntry<R>(id, Optional.empty(), Optional.empty(), false);
        }
    }
}
