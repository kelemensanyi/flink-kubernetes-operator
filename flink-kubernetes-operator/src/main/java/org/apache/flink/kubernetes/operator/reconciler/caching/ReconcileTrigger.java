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

import io.javaoperatorsdk.operator.OperatorException;
import io.javaoperatorsdk.operator.processing.event.Event;
import io.javaoperatorsdk.operator.processing.event.EventHandler;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;

/** Event source for triggering reconcile from CachingReconciler. */
public class ReconcileTrigger implements EventSource {

    // TODO: Check if EventSource is supposed to be used that way
    private EventHandler handler =
            (Event event) -> new RuntimeException("ReconcileTrigger handle not initialized");

    @Override
    public void setEventHandler(EventHandler handler) {
        this.handler = handler;
    }

    public void fire(ResourceID id) {
        handler.handleEvent(new Event(id));
    }

    @Override
    public void start() throws OperatorException {}

    @Override
    public void stop() throws OperatorException {}
}
