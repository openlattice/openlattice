

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
import java.util.UUID;
import java.util.stream.Collectors;

import com.openlattice.authorization.Principal;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.organizations.PrincipalSet;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import com.openlattice.conductor.codecs.odata.Table;
import com.kryptnostic.rhizome.cassandra.ColumnDef;

public abstract class PrincipalSetMapstore extends UUIDKeyMapstore<PrincipalSet> {
    protected final ColumnDef valueCol;

    public PrincipalSetMapstore(
            HazelcastMap map,
            Session session,
            Table table,
            ColumnDef keyCol,
            ColumnDef valueCol ) {
        super( map, session, table, keyCol );
        this.valueCol = valueCol;
    }

    @Override
    protected BoundStatement bind( UUID key, PrincipalSet value, BoundStatement bs ) {
        return bs
                .setUUID( keyCol.cql(), key )
                .setSet( valueCol.cql(),
                        value.stream().map( Principal::getId ).collect( Collectors.toSet() ),
                        String.class );
    }

}
