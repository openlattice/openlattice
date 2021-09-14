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

import com.openlattice.auth0.Auth0SecurityPod;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;

@Configuration
@EnableGlobalMethodSecurity(
        prePostEnabled = true )
@EnableWebSecurity(
        debug = false )
@EnableMetrics
public class DatastoreSecurityPod extends Auth0SecurityPod {

    @Override
    protected void authorizeRequests( HttpSecurity http ) throws Exception {
        //TODO: Lock these down
        http.authorizeRequests()
                .antMatchers( HttpMethod.OPTIONS ).permitAll()
                .antMatchers( HttpMethod.POST, "/datastore/principals/users/" ).authenticated()
                .antMatchers( HttpMethod.PUT, "/datastore/principals/users/*" ).authenticated()
                .antMatchers( HttpMethod.GET, "/datastore/principals/users/activate" ).authenticated()
                .antMatchers( HttpMethod.GET, "/datastore/collections/entity/type" ).permitAll()
                .antMatchers( HttpMethod.GET, "/datastore/app" ).permitAll()
                .antMatchers( HttpMethod.GET, "/datastore/edm/**" ).permitAll()
                .antMatchers( "/datastore/data/entitydata/*" ).permitAll()
                .antMatchers( "/datastore/codex/incoming/**" ).permitAll()
                .antMatchers( "/datastore/codex/media/*" ).permitAll()
                .antMatchers( "/datastore/**" ).authenticated();
    }

}
