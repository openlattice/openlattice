/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.datastore.pods;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openlattice.admin.AdminController;
import com.openlattice.codex.controllers.CodexController;
import com.openlattice.collection.CollectionsController;
import com.openlattice.data.DataApi;
import com.openlattice.datastore.analysis.controllers.AnalysisController;
import com.openlattice.datastore.apps.controllers.AppController;
import com.openlattice.datastore.authorization.controllers.AuthorizationsController;
import com.openlattice.datastore.data.controllers.DataController;
import com.openlattice.datastore.directory.controllers.PrincipalDirectoryController;
import com.openlattice.datastore.edm.controllers.EdmController;
import com.openlattice.datastore.permissions.controllers.PermissionsController;
import com.openlattice.datastore.requests.controllers.RequestsController;
import com.openlattice.datastore.search.controllers.PersistentSearchController;
import com.openlattice.datastore.search.controllers.SearchController;
import com.openlattice.organizations.controllers.OrganizationsController;
import com.openlattice.datastore.util.DataStoreExceptionHandler;
import com.openlattice.entitysets.controllers.EntitySetsController;
import com.openlattice.graph.controllers.GraphController;
import com.openlattice.organizations.controllers.DatasetController;
import com.openlattice.subscriptions.controllers.FeedsController;
import com.openlattice.subscriptions.controllers.SubscriptionController;
import com.openlattice.web.converters.CsvHttpMessageConverter;
import com.openlattice.web.converters.YamlHttpMessageConverter;
import com.openlattice.web.mediatypes.CustomMediaType;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import javax.inject.Inject;
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

@Configuration
@ComponentScan(
        basePackageClasses = { DataController.class, SearchController.class,
                PermissionsController.class, AuthorizationsController.class,
                PrincipalDirectoryController.class,
                EdmController.class, OrganizationsController.class,
                DataStoreExceptionHandler.class, EntitySetsController.class,
                RequestsController.class, AppController.class, GraphController.class,
                PersistentSearchController.class, AdminController.class,
                SubscriptionController.class, FeedsController.class, CodexController.class, CollectionsController.class,
                DatasetController.class, AnalysisController.class
        },
        includeFilters = @ComponentScan.Filter(
                value = { org.springframework.stereotype.Controller.class,
                        org.springframework.web.bind.annotation.RestControllerAdvice.class },
                type = FilterType.ANNOTATION ) )
@EnableAsync
@EnableMetrics(
        proxyTargetClass = true )
public class DatastoreMvcPod extends WebMvcConfigurationSupport {

    @Inject
    private ObjectMapper defaultObjectMapper;

    @Inject
    private DatastoreSecurityPod datastoreSecurityPod;

    @Override
    protected void configureMessageConverters( List<HttpMessageConverter<?>> converters ) {
        super.addDefaultHttpMessageConverters( converters );
        for ( HttpMessageConverter<?> converter : converters ) {
            if ( converter instanceof MappingJackson2HttpMessageConverter ) {
                MappingJackson2HttpMessageConverter jackson2HttpMessageConverter = (MappingJackson2HttpMessageConverter) converter;
                jackson2HttpMessageConverter.setObjectMapper( defaultObjectMapper );
            }
        }
        converters.add( new CsvHttpMessageConverter() );
        converters.add( new YamlHttpMessageConverter() );
    }

    // TODO(LATTICE-2346): We need to lock this down. Since all endpoints are stateless + authenticated this is more a
    // defense-in-depth measure.
    @SuppressFBWarnings(
            value = {"PERMISSIVE_CORS"},
            justification = "LATTICE-2346"
    )
    @Override
    protected void addCorsMappings( CorsRegistry registry ) {
        registry
                .addMapping( "/**" )
                .allowedMethods( "GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH" )
                .allowedOrigins( "*" );
        super.addCorsMappings( registry );
    }

    @Override
    protected void configureContentNegotiation( ContentNegotiationConfigurer configurer ) {
        configurer.parameterName( DataApi.FILE_TYPE )
                .favorParameter( true )
                .mediaType( "csv", CustomMediaType.TEXT_CSV )
                .mediaType( "json", MediaType.APPLICATION_JSON )
                .mediaType( "yaml", CustomMediaType.TEXT_YAML )
                .defaultContentType( MediaType.APPLICATION_JSON );
    }

    @Bean
    public AuthenticationManager authenticationManagerBean() throws Exception {
        return datastoreSecurityPod.authenticationManagerBean();
    }
}
