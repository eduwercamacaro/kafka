package org.apache.kafka.streams.state;

import org.apache.kafka.streams.processor.StateStore;

public interface KeyValueStateStore<K, V> extends KeyValueStore<K, V>, StateStore {
}
