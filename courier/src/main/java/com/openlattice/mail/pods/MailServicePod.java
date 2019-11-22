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

package com.openlattice.mail.pods;

import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.openlattice.mail.config.MailServiceConfig;
import com.openlattice.mail.config.MailServiceRequirements;
import com.openlattice.mail.services.MailRenderer;
import com.openlattice.mail.services.MailService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.inject.Inject;
import java.io.IOException;

/**
 * This is the plugin pod that will activate a mail service lambda
 *
 */
@Configuration
@Import( { MailServiceConfigurationPod.class } )
public class MailServicePod {

    @Inject
    private MailServiceRequirements requirements;

    @Inject
    private ConfigurationService config;

    @Inject
    private MailServiceConfig mailServiceConfig;

    @Inject
    private ApplicationContext context;

    @Bean
    public MailService mailService() throws IOException {
        return new MailService( mailServiceConfig, mailRenderer(), requirements.getEmailQueue() );
    }

    @Bean
    public MailRenderer mailRenderer() {
        return new MailRenderer();
    }

}