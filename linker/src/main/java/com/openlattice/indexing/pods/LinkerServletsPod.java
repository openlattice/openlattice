package com.openlattice.indexing.pods;

import com.google.common.collect.Lists;
import com.kryptnostic.rhizome.configuration.servlets.DispatcherServletConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LinkerServletsPod {

    @Bean
    public DispatcherServletConfiguration indexerServlet() {

        return new DispatcherServletConfiguration(
                "indexer",
                new String[]{ "/indexer/*" },
                1,
                Lists.newArrayList( LinkerMvcPod.class )
        );
    }
}
