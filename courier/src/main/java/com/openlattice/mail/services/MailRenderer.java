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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.openlattice.mail.RenderableEmailRequest;
import com.openlattice.mail.exceptions.InvalidTemplateException;
import com.openlattice.mail.templates.EmailTemplate;
import com.openlattice.mail.utils.TemplateUtils;
import jodd.mail.Email;
import jodd.mail.EmailAttachment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

public class MailRenderer {
    private Logger                   logger                        = LoggerFactory
                                                                           .getLogger( MailRenderer.class );

    // TODO: Move to Dynamic Blacklist
    private static final Set<String> REGISTRATION_DOMAIN_BLACKLIST = Sets.newHashSet( "someblacklisteddomain.com" );

    @VisibleForTesting
    public static boolean isNotBlacklisted( String to ) {
        return !REGISTRATION_DOMAIN_BLACKLIST.stream().anyMatch( to::contains );
    }

    protected Set<Email> renderEmail( RenderableEmailRequest emailRequest ) {
        Iterable<String> toAddresses = Iterables.filter( Arrays.asList( emailRequest.getTo() ),
                MailRenderer::isNotBlacklisted );
        logger.info( "filtered e-mail addresses that are blacklisted." );

        if ( Iterables.size( toAddresses ) < 1 ) {
            logger.error( "Must include at least one valid e-mail address.");
            return ImmutableSet.of();
        }
        String template;
        try {
            template = TemplateUtils.loadTemplate( emailRequest.getTemplatePath() );
        } catch ( IOException e ) {
            throw new InvalidTemplateException(
                    "Invalid Email Template: " + emailRequest.getTemplatePath(),
                    e );
        }
        String templateHtml = TemplateUtils.DEFAULT_TEMPLATE_COMPILER
                .compile( template )
                .execute( emailRequest.getTemplateObjs().orElse( new Object() ) );

        /*
         * when someone invites multiple people, we want to spool an individual invite for each person. as such, we need
         * to create a multiple Email objects instead of one Email object with multiple "To" fields to avoid having
         * multiple emails appear in the "To" field in an email client like Gmail.
         */
        Set<Email> emailSet = Sets.newHashSet();
        for ( String toAddress : toAddresses ) {

            Email email = Email.create()
                    .from( emailRequest.getFrom().orElse( EmailTemplate.getCourierEmailAddress() ) )
                    .to( toAddress )
                    .subject( emailRequest.getSubject().orElse( "" ) )
                    .htmlMessage( templateHtml );
            if ( emailRequest.getByteArrayAttachment().isPresent() ) {
                EmailAttachment[] attachments = emailRequest.getByteArrayAttachment().get();
                for ( EmailAttachment attachment : attachments ) {
                    email.attachment( attachment );
                }
            }

            if ( emailRequest.getAttachmentPaths().isPresent() ) {
                String[] paths = emailRequest.getAttachmentPaths().get();
                for ( String path : paths ) {
                    email.attachment( EmailAttachment.with().content( path ) );
                }
            }

            emailSet.add( email );
        }
        return emailSet;
    }

}
