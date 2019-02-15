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

package com.openlattice.mail.gravatar;

import com.openlattice.mail.requirements.ScribeRequirements;
import com.openlattice.mail.templates.EmailTemplate;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import com.openlattice.mail.MailServiceClient;
import com.openlattice.mail.RenderableEmailRequest;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

public class Gravatar {
    private final static int    DEFAULT_SIZE       = 80;
    private final static String GRAVATAR_URL       = "http://www.gravatar.com/avatar/";
    private final static String DEFAULT_AVATAR_URL = "https://www.openlattice.com/static/media/default_avatar.png";
    private int                 size               = DEFAULT_SIZE;
    
    @Inject
    private ScribeRequirements requirements;

    public void setSize( int sizeInPixels ) {
        if ( sizeInPixels >= 1 && sizeInPixels <= 512 ) {
            this.size = sizeInPixels;
        }
    }

    @SuppressFBWarnings(value="WEAK_MESSAGE_DIGEST_MD5",justification = "Gravatar uses md5... that's not good")
    public URL getUrl( String email ) {
        String emailHash = DigestUtils.md5Hex( email.toLowerCase().trim() );
        String params = formatUrlParameters();
        try {
            URL url = new URL( GRAVATAR_URL + emailHash + params );
            return url;
        } catch ( MalformedURLException e ) {
            logErrorAndSendEmailReport( e );
        }
        return null;
    }

    private String formatUrlParameters() {
        List<String> params = new ArrayList<String>();
        if ( size != DEFAULT_SIZE )
            params.add( "s=" + size );
        params.add( "d=" + DEFAULT_AVATAR_URL );
        if ( params.isEmpty() ) {
            return "";
        } else {
            return "?" + StringUtils.join( params.iterator(), "&" );
        }
    }
    
    private void logErrorAndSendEmailReport( Exception ex ) {
        
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ex.printStackTrace( pw );
        
        System.err.println( sw.toString() );
        
        MailServiceClient mailService = new MailServiceClient( requirements.getEmailQueue() );
        RenderableEmailRequest emailRequest = new RenderableEmailRequest(
                Optional.of( EmailTemplate.getCourierEmailAddress() ),
                new String[] { "outage@openlattice.com" },
                Optional.absent(),
                Optional.absent(),
                EmailTemplate.INTERNAL_ERROR.getPath(),
                Optional.of( EmailTemplate.INTERNAL_ERROR.getSubject() ),
                Optional.of( ImmutableMap.of( "message", sw.toString() ) ),
                Optional.absent(),
                Optional.absent());
        mailService.spool( emailRequest );
        
    }
}
