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

import java.util.function.Supplier;

/** Utility methods for the Supplier interface. */
public class SupplierSupport {
    public static <T> Supplier<T> withErrorHandler(PossiblyFailingSupplier<T> supplier) {
        return () -> {
            try {
                return supplier.get();
            } catch (Throwable t) {
                // TODO proper error handling, logging
                t.printStackTrace(System.err);
                throw new RuntimeException(t);
            }
        };
    }

    /** A Supplier that might throw an Exception. */
    @FunctionalInterface
    public interface PossiblyFailingSupplier<T> {
        T get() throws Exception;
    }
}
