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

import java.security.Security;

import org.junit.Before;

import com.openlattice.mail.config.MailServiceConfig;
import com.icegreen.greenmail.util.DummySSLSocketFactory;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetupTest;

public class GreenMailTest {

    protected static final int         DEFAULT_WAIT_TIME = 2000;

    protected static final String      HOST              = "localhost";
    protected static final String      USERNAME          = "username";
    protected static final String      PASSWORD          = "password";

    protected static final int         PORT              = ServerSetupTest.SMTPS.getPort();

    protected static final GreenMail         greenMailServer;
    protected static final MailServiceConfig testMailServiceConfig;

    static  {

        // needed to avoid "javax.net.ssl.SSLHandshakeException: Received fatal alert: certificate_unknown"
        Security.setProperty( "ssl.SocketFactory.provider", DummySSLSocketFactory.class.getName() );

        greenMailServer = new GreenMail( ServerSetupTest.SMTPS );

        testMailServiceConfig = new MailServiceConfig(
                HOST,
                PORT,
                USERNAME,
                PASSWORD );

        greenMailServer.start();
    }

    @Before
    public void startGreenMail() {


    }

}