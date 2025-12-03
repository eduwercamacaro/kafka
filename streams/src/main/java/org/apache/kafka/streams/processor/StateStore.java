/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor;

import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;

/**
 * A storage engine for managing state maintained by a stream processor.
 * <p>
 * If the store is implemented as a persistent store, it <em>must</em> use the store name as directory name and write
 * all data into this store directory.
 * The store directory must be created with the state directory.
 * The state directory can be obtained via {@link ProcessorContext#stateDir() #stateDir()} using the
 * {@link ProcessorContext} provided via {@link #init(StateStoreContext, Store) init(...)}.
 * <p>
 * Using nested store directories within the state directory isolates different state stores.
 * If a state store would write into the state directory directly, it might conflict with others state stores and thus,
 * data might get corrupted and/or Streams might fail with an error.
 * Furthermore, Kafka Streams relies on using the store name as store directory name to perform internal cleanup tasks.
 * <p>
 * This interface does not specify any query capabilities, which, of course,
 * would be query engine specific. Instead, it just specifies the minimum
 * functionality required to reload a storage engine from its changelog as well
 * as basic lifecycle management.
 */
public interface StateStore extends Store {


    /**
     * Initializes this state store.
     * <p>
     * The implementation of this function must register the root store in the stateStoreContext via the
     * {@link StateStoreContext#register(StateStore, StateRestoreCallback, CommitCallback)} function, where the
     * first {@link StateStore} parameter should always be the passed-in {@code root} object, and
     * the second parameter should be an object of user's implementation
     * of the {@link StateRestoreCallback} interface used for restoring the state store from the changelog.
     * <p>
     * Note that if the state store engine itself supports bulk writes, users can implement another
     * interface {@link BatchingStateRestoreCallback} which extends {@link StateRestoreCallback} to
     * let users implement bulk-load restoration logic instead of restoring one record at a time.
     *
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     */
    void init(final StateStoreContext stateStoreContext, final StateStore root);

    /**
     * Flush any cached data
     */
    void flush();

    /**
     * Close the storage engine.
     * Note that this function needs to be idempotent since it may be called
     * several times on the same state store.
     * <p>
     * Users only need to implement this function but should NEVER need to call this api explicitly
     * as it will be called by the library automatically when necessary
     */
    void close();

    /**
     * Returns the position the state store is at with respect to the input topic/partitions
     */
    @Evolving
    default Position getPosition() {
        throw new UnsupportedOperationException(
            "getPosition is not implemented by this StateStore (" + getClass() + ")"
        );
    }
}
