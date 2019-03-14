package com.adventuresincs.examples.utils;

import org.apache.flink.api.common.functions.Partitioner;

public class ImbalancedPartitioner<T> implements Partitioner<T> {
    private static final long serialVersionUID = 1L;

    @Override
    public int partition(Object key, int numPartitions) {
        return 0;
    }

}
