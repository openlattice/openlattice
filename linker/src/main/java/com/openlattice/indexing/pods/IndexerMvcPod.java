package com.openlattice.indexing.pods;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openlattice.data.DataApi;
import com.openlattice.linking.controllers.LinkingFeedbackController;
import com.openlattice.linking.controllers.RealtimeLinkingController;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;

import javax.inject.Inject;
import java.util.List;

@Configuration
@ComponentScan(
        basePackageClasses = { RealtimeLinkingController.class, LinkingFeedbackController.class },
        includeFilters = @ComponentScan.Filter(
                value = { org.springframework.stereotype.Controller.class,
                        org.springframework.web.bind.annotation.RestControllerAdvice.class },
                type = FilterType.ANNOTATION ) )

@EnableAsync
@EnableMetrics( proxyTargetClass = true )
public class IndexerMvcPod extends WebMvcConfigurationSupport {
    @Inject
    private ObjectMapper defaultObjectMapper;

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

    @Override
    protected void configureMessageConverters( List<HttpMessageConverter<?>> converters ) {
        super.addDefaultHttpMessageConverters( converters );
        for ( HttpMessageConverter<?> converter : converters ) {
            if ( converter instanceof MappingJackson2HttpMessageConverter ) {
                MappingJackson2HttpMessageConverter jackson2HttpMessageConverter =
                        ( MappingJackson2HttpMessageConverter ) converter;
                jackson2HttpMessageConverter.setObjectMapper( defaultObjectMapper );
            }
        }
    }

    @Override
    protected void configureContentNegotiation( ContentNegotiationConfigurer configurer ) {
        configurer.parameterName( DataApi.FILE_TYPE ).defaultContentType( MediaType.APPLICATION_JSON );
    }

    @Bean
    public AuthenticationManager authenticationManagerBean() throws Exception {
        return indexerSecurityPod.authenticationManagerBean();
    }
}
