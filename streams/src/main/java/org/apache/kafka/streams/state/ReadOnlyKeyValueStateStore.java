package org.apache.kafka.streams.state;

import org.apache.kafka.streams.processor.StateStore;

public interface ReadOnlyKeyValueStateStore<K, V> extends ReadOnlyKeyValueStore<K, V>, StateStore {
}
