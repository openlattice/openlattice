/*
 * Copyright (C) 2017. OpenLattice, Inc
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

package com.openlattice.authorization;

import com.google.common.base.MoreObjects;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.datastore.util.Util;
import com.openlattice.hazelcast.HazelcastMap;
import java.security.SecureRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DbCredentialService {
    private static final Logger logger = LoggerFactory.getLogger( DbCredentialService.class );

    private static final String upper   = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String lower   = upper.toLowerCase();
    private static final String digits  = "0123456789";
    private static final String special = "!@#$%^&*()";
    private static final String source  = upper + lower + digits + special;
    private static final char[] srcBuf  = source.toCharArray();

    private static final int CREDENTIAL_LENGTH = 20;

    private static final SecureRandom r = new SecureRandom();

    private final IMap<String, String> dbcreds;

    public DbCredentialService( HazelcastInstance hazelcastInstance, PostgresUserApi pgUserApi ) {
        this.dbcreds = HazelcastMap.DB_CREDS.getMap( hazelcastInstance );
    }

    public String getDbCredential( String userId ) {
        return Util.getSafely( dbcreds, userId );
    }

    public String getOrCreateUserCredentials( String userId ) {
        logger.info( "Generating credentials for user id {}", userId );
        String cred = generateCredential();
        logger.info( "Generated credentials for user id {}", userId );
        return MoreObjects.firstNonNull( dbcreds.putIfAbsent( userId, cred ), cred );
    }

    public void deleteUserCredential( String userId ) {
        dbcreds.delete( userId );
    }

    public String rollUserCredential( String userId ) {
        String cred = generateCredential();
        dbcreds.set( userId, cred );
        return cred;
    }

    private String generateCredential() {
        char[] cred = new char[ CREDENTIAL_LENGTH ];
        for ( int i = 0; i < CREDENTIAL_LENGTH; ++i ) {
            cred[ i ] = srcBuf[ r.nextInt( srcBuf.length ) ];
        }
        return new String( cred );
    }
}