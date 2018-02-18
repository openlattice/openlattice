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

import com.openlattice.mail.templates.EmailTemplate;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.mail.Message;
import javax.mail.MessagingException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.openlattice.mail.RenderableEmailRequest;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.hazelcast.core.IQueue;

public class MailServiceTest extends GreenMailTest {
    private static final IQueue<RenderableEmailRequest> emailRequests = Mockito.mock( IQRER.class );
    private static MailService                          mailService;
    private static MailRenderer                         renderer      = new MailRenderer();

    @BeforeClass
    public static void beforeClass() throws Exception {
        mailService = new MailService( testMailServiceConfig, renderer, emailRequests );
    }

    @Test
    public void sendEmailTest() throws MessagingException, InterruptedException {
        String[] toAddresses = new String[] { "GoJIRA <jira@openlattice.com>", "Master Chief <mc@openlattice.com>" };
        RenderableEmailRequest emailRequest = new RenderableEmailRequest(
                Optional.of( EmailTemplate.getCourierEmailAddress() ),
                toAddresses,
                Optional.absent(),
                Optional.absent(),
                EmailTemplate.SERVICE_OUTAGE.getPath(),
                Optional.of( EmailTemplate.SERVICE_OUTAGE.getSubject() ),
                Optional.of( ImmutableMap
                        .of( "name", "Master Chief", "avatar-path", "the path", "registration-url", "test" ) ),
                Optional.absent(),
                Optional.absent() );

        CountDownLatch latch = new CountDownLatch( 1 );
        Lock lock = new ReentrantLock();
        Mockito.when( emailRequests.take() )
                .then( invocation -> {
                    if ( lock.tryLock() ) {
                        latch.countDown();
                        return emailRequest;
                    } else {
                        lock.lock();
                        return null;
                    }
                } );
        Executor executor = Executors.newSingleThreadExecutor();
        executor.execute( mailService::processEmailRequestsQueue ); // Will trigger rendering
        latch.await();

        Mockito.verify( emailRequests, Mockito.atLeastOnce() ).take();

        // waitForIncomingEmail() is useful if sending is done asynchronously in a separate thread
        Assert.assertTrue( greenMailServer.waitForIncomingEmail( DEFAULT_WAIT_TIME, 2 ) );

        Message[] emails = greenMailServer.getReceivedMessages();
        Assert.assertEquals( toAddresses.length, emails.length );

        for ( int i = 0; i < emails.length; ++i ) {
            Message email = emails[ i ];

            Assert.assertEquals( EmailTemplate.SERVICE_OUTAGE.getSubject(), email.getSubject() );

            Assert.assertEquals( 1, email.getAllRecipients().length );
            Assert.assertNull( email.getRecipients( Message.RecipientType.CC ) );
            Assert.assertNull( email.getRecipients( Message.RecipientType.BCC ) );

            Assert.assertEquals( 1, email.getHeader( "From" ).length );
            Assert.assertEquals( EmailTemplate.getCourierEmailAddress(), email.getHeader( "From" )[ 0 ] );

            Assert.assertEquals( 1, email.getHeader( "To" ).length );
            // For efficiency, I update the sendEmail method to spool out all emails through same session.
            // So we cannot guarantee the receiving order.
            // Assert.assertEquals( toAddresses[ i ], email.getHeader( "To" )[ 0 ] );
        }

    }

    @Test(
        expected = NullPointerException.class )
    public void testBadRequest_NullEmailRequest() throws IOException {
        mailService.sendEmailAfterRendering( null, null );
    }

    @Test
    public void testJustinIsBlacklisted() {
        Assert.assertFalse( MailRenderer.isNotBlacklisted( "foo@someblacklisteddomain.com" ) );
    }

    @Test
    public void testOthersAreNotBlacklisted() {
        Assert.assertTrue( MailRenderer.isNotBlacklisted( "foo@safe.com" ) );
    }

    static interface IQRER extends IQueue<RenderableEmailRequest> {

    }
}
