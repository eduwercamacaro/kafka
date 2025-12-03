package org.apache.kafka.streams.processor;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;

public interface Store {

    /**
     * The name of this store.
     * @return the storage name
     */
    String name();

    /**
     * Return if the storage is persistent or not.
     *
     * @return  {@code true} if the storage is persistent&mdash;{@code false} otherwise
     */
    boolean persistent();

    /**
     * Execute a query. Returns a QueryResult containing either result data or
     * a failure.
     * <p>
     * If the store doesn't know how to handle the given query, the result
     * shall be a {@link FailureReason#UNKNOWN_QUERY_TYPE}.
     * If the store couldn't satisfy the given position bound, the result
     * shall be a {@link FailureReason#NOT_UP_TO_BOUND}.
     * <p>
     * Note to store implementers: if your store does not support position tracking,
     * you can correctly respond {@link FailureReason#NOT_UP_TO_BOUND} if the argument is
     * anything but {@link PositionBound#unbounded()}. Be sure to explain in the failure message
     * that bounded positions are not supported.
     * <p>
     * @param query The query to execute
     * @param positionBound The position the store must be at or past
     * @param config Per query configuration parameters, such as whether the store should collect detailed execution
     * info for the query
     * @param <R> The result type
     */
    @InterfaceStability.Evolving
    default <R> QueryResult<R> query(
            final Query<R> query,
            final PositionBound positionBound,
            final QueryConfig config) {
        // If a store doesn't implement a query handler, then all queries are unknown.
        return QueryResult.forUnknownQueryType(query, this);
    }

    /**
     * Is this store open for reading and writing
     * @return {@code true} if the store is open
     */
    boolean isOpen();


}
