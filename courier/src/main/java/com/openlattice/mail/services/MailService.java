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

package com.openlattice.mail.services;

import com.google.common.base.Preconditions;
import com.hazelcast.collection.IQueue;
import com.openlattice.mail.RenderableEmailRequest;
import com.openlattice.mail.config.MailServiceConfig;
import jodd.mail.Email;
import jodd.mail.MailException;
import jodd.mail.SendMailSession;
import jodd.mail.SmtpServer;
import jodd.mail.SmtpSslServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Set;

public class MailService {
    private final MailRenderer                   mailRenderer;
    private final IQueue<RenderableEmailRequest> emailRequests;
    private       Logger                         logger = LoggerFactory
            .getLogger( MailService.class );
    @SuppressWarnings( "rawtypes" )
    private       SmtpServer                     smtpServer;

    public MailService(
            MailServiceConfig config,
            MailRenderer mailRenderer,
            IQueue<RenderableEmailRequest> emailRequests ) {
        this.emailRequests = emailRequests;
        configureSmtpServer( config );
        this.mailRenderer = mailRenderer;
        logger.info( "Mail Service successfully configured and initialized!" );
    }

    public void configureSmtpServer( MailServiceConfig config ) {
        Preconditions.checkNotNull( config, "Mail Service configuration cannot be null." );
        smtpServer = SmtpSslServer
                .create()
                .host( config.getSmtpHost() )
                .port( config.getSmtpPort() )
                .auth( config.getUsername(), config.getPassword() )
                .ssl( true )
                .buildSmtpMailServer();
        // TODO: "javax.net.ssl.SSLException: Unrecognized SSL message, plaintext connection?"
        // TODO: figure out why we get above exception when plaintextOverTLS is false and port is 587 (works for 465)
        // .plaintextOverTLS(false)
        // .startTlsRequired(true);
    }

    protected Set<Email> renderEmail( RenderableEmailRequest emailRequest ) {
        return mailRenderer.renderEmail( emailRequest );
    }

    @Async
    void sendEmailAfterRendering( RenderableEmailRequest emailRequest, SendMailSession session ) {
        Set<Email> emailSet = renderEmail( emailRequest );
        Preconditions.checkNotNull( emailSet, "Email cannot be null." );
        for ( Email email : emailSet ) {
            session.sendMail( email );
        }
    }

    /**
     * By design under heavy load (not all emails rendered within 15 seconds) this will start dedicating more and more
     * threads to rendering outgoing e-mails queue.
     */
    @Scheduled( fixedRate = 30000 )
    public void processEmailRequestsQueue() {
        SendMailSession session = smtpServer.createSession();
        try {
            session.open();
        } catch ( MailException ex ) {
            logger.error( "Failure opening email session: {}", ex.getMessage());
        }

        while ( !emailRequests.isEmpty() ) {
            RenderableEmailRequest emailRequest = null;
            try {
                emailRequest = emailRequests.take();
                sendEmailAfterRendering( emailRequest, session );
            } catch ( InterruptedException e ) {
                logger.error( "Interrupted while waiting on e-mails to render." );
            }
        }

        session.close();
    }

}