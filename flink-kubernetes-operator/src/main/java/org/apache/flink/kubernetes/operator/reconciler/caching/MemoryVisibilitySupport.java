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

import java.util.concurrent.ConcurrentHashMap;

/** Utility class for thread interoperability. */
public class MemoryVisibilitySupport {
    // TODO: implement cleanup
    private static final ConcurrentHashMap<ResourceID, Object> locks = new ConcurrentHashMap<>();

    public static void sync(ResourceID id) {
        synchronized (locks.computeIfAbsent(id, id0 -> new Object())) {
            // This is just a memory barrier
            assert true;
        }
    }
}
