package com.openlattice.indexing.pods;

import com.hazelcast.core.HazelcastInstance;
import com.openlattice.graph.processing.processors.GraphProcessor;
import com.openlattice.graph.processing.processors.SharedGraphProcessors;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.Set;

@Configuration
@ComponentScan(
        basePackageClasses = { SharedGraphProcessors.class },
        includeFilters = @ComponentScan.Filter(
                value = { Component.class },
                type = FilterType.ANNOTATION ) )
public class GraphProcessorPod {

    @Inject
    private Set<GraphProcessor> graphProcessors;

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private HikariDataSource hikariDataSource;

}
