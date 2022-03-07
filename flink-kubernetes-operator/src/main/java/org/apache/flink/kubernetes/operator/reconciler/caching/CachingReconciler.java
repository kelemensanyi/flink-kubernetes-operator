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

import org.apache.flink.kubernetes.operator.reconciler.caching.Cache.CacheEntry;
import org.apache.flink.kubernetes.operator.utils.SerializationUtils;

import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.kubernetes.operator.reconciler.caching.SupplierSupport.withErrorHandler;

/** Caching reconciler. */
public class CachingReconciler<
                S,
                T,
                R extends CustomResource<S, T>,
                E extends Reconciler<R> & ErrorStatusHandler<R> & EventSourceInitializer<R>>
        implements Reconciler<R>, ErrorStatusHandler<R>, EventSourceInitializer<R> {

    private final E wrappedReconciler;

    private final Cache<R> cache = new Cache<>();

    public CachingReconciler(E wrappedReconciler) {
        this.wrappedReconciler = wrappedReconciler;
    }

    @Override
    public UpdateControl<R> reconcile(R resource, Context context) {
        ResourceID resourceId = ResourceID.fromResource(resource);

        CacheEntry<R> entry = cache.get(resourceId);

        if (entry.getPendingResult().isPresent()) {
            return processPendingResult(resource, entry);
        }

        if (entry.asyncRunning) {
            // Already in progress
            return UpdateControl.noUpdate();
        }

        MemoryVisibilitySupport.sync(resourceId);

        final R copiedRes = SerializationUtils.deepCopy(resource);

        Supplier<UpdateControl<R>> reconcileCall =
                withErrorHandler(
                        () -> {
                            // It is only to enforce that the thread running here sees
                            // everything that was visible for the thread executing
                            // CachingReconciler::reconcile
                            MemoryVisibilitySupport.sync(resourceId);

                            // TODO: `context` might not be ThreadSafe or usable after sync return
                            return wrappedReconciler.reconcile(copiedRes, context);
                        });

        Consumer<UpdateControl<R>> asyncHandlerSuccess =
                updateControl -> {
                    cache.put(
                            resourceId,
                            entry.withPendingResult(new ReconcileResult(copiedRes, updateControl))
                                    .withValue(copiedRes)
                                    .withAsyncRunning(false));
                    triggerReconcileAgain(resourceId);
                };

        Consumer<Exception> asyncHandlerFailure =
                exception -> {
                    cache.put(resourceId, entry.withAsyncRunning(false));
                    triggerReconcileAgain(resourceId);
                };

        int gracePeriodMillis = 500; // TODO: from config
        Optional<UpdateControl<R>> syncResult =
                SyncOrAsyncExecutor.runSyncOrAsync(
                        reconcileCall, gracePeriodMillis, asyncHandlerSuccess, asyncHandlerFailure);

        if (syncResult.isPresent()) {
            return syncResult.get();
        }

        cache.put(resourceId, entry.withAsyncRunning(true));
        return UpdateControl.noUpdate();
    }

    private UpdateControl<R> processPendingResult(R resource, CacheEntry<R> entry) {
        ResourceID resourceId = entry.getId();
        ReconcileResult<S, T, R> result = entry.pendingResult.get();
        cache.put(resourceId, entry.withEmptyPendingResult());
        if (hasSameVersion(resource, result.resource)) {
            resource.setSpec(result.resource.getSpec());
            resource.setStatus(result.resource.getStatus());
            return result.updateControl;
        } else {
            // Resource was updated during async reconciliation
            triggerReconcileAgain(resourceId);
            return UpdateControl.noUpdate();
        }
    }

    private boolean hasSameVersion(R a, R b) {
        String aVersion = a.getMetadata().getResourceVersion();
        String bVersion = b.getMetadata().getResourceVersion();
        return Objects.equals(aVersion, bVersion);
    }

    @Override
    public DeleteControl cleanup(R resource, Context context) {
        // TODO: async cleanup
        return wrappedReconciler.cleanup(resource, context);
    }

    @Override
    public Optional<R> updateErrorStatus(R resource, RetryInfo retryInfo, RuntimeException e) {
        return wrappedReconciler.updateErrorStatus(resource, retryInfo, e);
    }

    @Override
    public List<EventSource> prepareEventSources(EventSourceContext<R> context) {
        List<EventSource> all = new LinkedList<>(wrappedReconciler.prepareEventSources(context));
        all.add(trigger);
        return all;
    }

    private final ReconcileTrigger trigger = new ReconcileTrigger();

    private void triggerReconcileAgain(ResourceID id) {
        trigger.fire(id);
    }
}
