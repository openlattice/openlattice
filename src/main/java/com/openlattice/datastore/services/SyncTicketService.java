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

package com.openlattice.datastore.services;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.data.TicketKey;
import com.openlattice.datastore.util.Util;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.security.access.AccessDeniedException;

;

public class SyncTicketService {
    private final IMap<TicketKey, UUID>             authorizedEntitySets;
    private final IMap<TicketKey, DelegatedUUIDSet> authorizedProperties;

    public SyncTicketService( HazelcastInstance hazelcast ) {
        authorizedEntitySets = hazelcast.getMap( HazelcastMap.ENTITY_SET_TICKETS.name() );
        authorizedProperties = hazelcast.getMap( HazelcastMap.ENTITY_SET_PROPERTIES_TICKETS.name() );
    }

    public UUID acquireTicket( Principal principal, UUID entitySetId, Set<UUID> propertyIds ) {
        checkArgument( principal.getType().equals( PrincipalType.USER ), "Only users can retrieve tickets." );
        TicketKey key = new TicketKey( principal.getId() );

        while ( authorizedEntitySets.putIfAbsent( key, entitySetId, 24, TimeUnit.HOURS ) != null ) {
            key = new TicketKey( principal.getId() );
        }
        authorizedProperties.set( key, DelegatedUUIDSet.wrap( propertyIds ), 24, TimeUnit.HOURS );
        return key.getTicket();
    }

    public void releaseTicket( Principal principal, UUID ticketId ) {
        TicketKey key = ticketKey( principal, ticketId );
        authorizedEntitySets.delete( key );
        authorizedProperties.delete( key );
    }

    private TicketKey ticketKey( Principal principal, UUID ticketId ) {
        return new TicketKey( checkNotNull( principal ).getId(), checkNotNull( ticketId ) );
    }

    public Set<UUID> getAuthorizedProperties( Principal principal, UUID ticket ) {
        TicketKey key = ticketKey( principal, ticket );
        if ( authorizedEntitySets.containsKey( key ) ) {
            return Util.getSafely( authorizedProperties, key ).unwrap();
        }
        throw new AccessDeniedException( "Unable to authorized access to resource" );
    }

    public UUID getAuthorizedEntitySet( Principal currentUser, UUID ticket ) {
        UUID entitySetId = Util.getSafely( authorizedEntitySets, ticketKey( currentUser, ticket ) );
        if ( entitySetId == null ) {
            throw new AccessDeniedException( "Unable to authorized access to resource" );
        } else {
            return entitySetId;
        }
    }
}
