package com.openlattice.indexing.pods;

import com.hazelcast.core.HazelcastInstance;
import com.openlattice.BackgroundGraphProcessingService;
import com.openlattice.data.storage.PostgresEntityDataQueryService;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.graph.processing.GraphProcessingService;
import com.openlattice.graph.processing.processors.GraphProcessor;
import com.openlattice.graph.processing.processors.SharedGraphProcessors;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
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
    private EdmManager edmManager;

    @Inject
    private PostgresEntityDataQueryService dataQueryService;

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private HikariDataSource hikariDataSource;

//    @Bean
//    public GraphProcessingService graphProcessingService() {
//        return new GraphProcessingService(edmManager, hikariDataSource, hazelcastInstance, graphProcessors);
//    }
//
//    @Bean
//    public BackgroundGraphProcessingService backgroundGraphProcessingService() {
//        return new BackgroundGraphProcessingService(graphProcessingService());
//    }
}
