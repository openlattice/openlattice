package com.openlattice.indexing.pods;

import com.openlattice.linking.controllers.RealtimeLinkingController;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;

import javax.inject.Inject;

@Configuration
@ComponentScan(
        basePackageClasses = { RealtimeLinkingController.class },
        includeFilters = @ComponentScan.Filter(
                value = {org.springframework.stereotype.Controller.class,
                        org.springframework.web.bind.annotation.RestControllerAdvice.class},
                type = FilterType.ANNOTATION))

@EnableAsync
public class IndexerMvcPod extends WebMvcConfigurationSupport {

    @Inject
    private IndexerSecurityPod indexerSecurityPod;

    @Override
    protected void addCorsMappings( CorsRegistry registry ) {
        registry
                .addMapping( "/**" )
                .allowedMethods( "GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH" )
                .allowedOrigins( "*" );
        super.addCorsMappings( registry );
    }

    @Bean
    public AuthenticationManager authenticationManagerBean() throws Exception {
        return indexerSecurityPod.authenticationManagerBean();
    }
}
