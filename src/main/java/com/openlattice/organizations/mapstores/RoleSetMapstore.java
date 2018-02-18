

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

package com.openlattice.organizations.mapstores;

import com.openlattice.hazelcast.HazelcastMap;
import java.util.Set;
import java.util.stream.Collectors;

import com.openlattice.conductor.codecs.odata.Table;
import org.apache.commons.lang.RandomStringUtils;

import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.organizations.PrincipalSet;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.cassandra.ColumnDef;

public class RoleSetMapstore extends PrincipalSetMapstore {

    public RoleSetMapstore( HazelcastMap map, Session session, Table table, ColumnDef keyCol, ColumnDef valueCol ) {
        super( map, session, table, keyCol, valueCol );
    }

    @Override
    protected PrincipalSet mapValue( ResultSet rs ) {
        Row r = rs.one();
        if ( r == null ) {
            return null;
        }
        Set<String> roles = r.getSet( valueCol.cql(), String.class );
        return PrincipalSet.wrap(
                roles.stream().map( role -> new Principal( PrincipalType.ROLE, role ) ).collect( Collectors.toSet() ) );
    }

    @Override
    public PrincipalSet generateTestValue() {
        return PrincipalSet
                .wrap( ImmutableSet.of( new Principal( PrincipalType.ROLE, RandomStringUtils.randomAlphanumeric( 5 ) ),
                        new Principal( PrincipalType.ROLE, RandomStringUtils.randomAlphanumeric( 5 ) ),
                        new Principal( PrincipalType.ROLE, RandomStringUtils.randomAlphanumeric( 5 ) ) ) );
    }

}
