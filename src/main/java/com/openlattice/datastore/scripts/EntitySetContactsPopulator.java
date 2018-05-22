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

package com.openlattice.datastore.scripts;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Iterables;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.authorization.util.AuthorizationUtils;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.directory.UserDirectoryService;
import com.openlattice.edm.EntitySet;
import com.openlattice.hazelcast.HazelcastMap;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;

public class EntitySetContactsPopulator implements Serializable {
    private static final long serialVersionUID = -6252192257512539448L;
    private final IMap<UUID, EntitySet> entitySets;
    private       String                keyspace;
    private       Session               session;
    private       EdmManager            dms;
    private       UserDirectoryService  uds;

    public EntitySetContactsPopulator(
            String keyspace,
            Session session,
            EdmManager dms,
            UserDirectoryService uds,
            HazelcastInstance hazelcastInstance ) {
        this.keyspace = keyspace;
        this.session = session;
        this.dms = dms;
        this.uds = uds;
        this.entitySets = hazelcastInstance.getMap( HazelcastMap.ENTITY_SETS.name() );
    }

    public void run() {
        //        StreamUtil.stream( dms.getEntitySets() )
        //                .filter( es -> es.getContacts() == null || es.getContacts().isEmpty() )
        //                .forEach( es -> {
        //                    Set<String> contacts = StreamUtil.stream( getOwnerForEntitySet( Arrays.asList( es.getId() ) ) )
        //                            .map( principal -> getUserAsString( principal ) )
        //                            .collect( Collectors.toSet() );
        //
        //                    if( contacts.isEmpty() ){
        //                        contacts = ImmutableSet.of( "No contacts found" );
        //                    }
        //
        //                    entitySets.executeOnKey( es.getId(), new UpdateEntitySetContactsProcessor( contacts ) );
        //        } );
    }

    private Iterable<Principal> getOwnerForEntitySet( List<UUID> entitySetId ) {
        ResultSet rs = session.execute( getOwnerQuery().bind().setList( CommonColumns.ACL_KEYS.cql(),
                entitySetId,
                UUID.class ) );
        return Iterables.transform( rs, AuthorizationUtils::getPrincipalFromRow );
    }

    private PreparedStatement getOwnerQuery() {
        return session.prepare( QueryBuilder
                .select()
                .from( keyspace, Table.PERMISSIONS.getName() ).allowFiltering()
                .where( QueryBuilder.eq( CommonColumns.ACL_KEYS.cql(),
                        CommonColumns.ACL_KEYS.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.PRINCIPAL_TYPE.cql(), PrincipalType.USER ) )
                .and( QueryBuilder.contains( CommonColumns.PERMISSIONS.cql(), Permission.OWNER ) ) );
    }

    private String getUserAsString( Principal principal ) {
        return uds.getUser( principal.getId() ).getUsername();
    }

}
