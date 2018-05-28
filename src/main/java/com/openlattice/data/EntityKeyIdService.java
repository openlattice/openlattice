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
package com.openlattice.data;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public interface EntityKeyIdService {

    /**
     * Retrieves the assigned id for an entity key. Assigns one if entity key hasn't been assigned.
     *
     * @param entityKey The entity key for which to retrieve an assigned id.
     * @return The id assigned to entity key.
     */
    UUID getEntityKeyId( EntityKey entityKey );

    UUID getEntityKeyId( UUID entitySetId, String entityId );

    Map<EntityKey, UUID> getEntityKeyIds( Set<EntityKey> entityKeys );

    Set<Entry<EntityKey, UUID>> getEntityKeyEntries( Set<UUID> entityKeyIds );

    EntityKey getEntityKey( UUID entityKeyId );

    Map<UUID, EntityKey> getEntityKeys( Set<UUID> entityKeyIds );

    Map<String, UUID> assignEntityKeyIds( UUID entitySetId, Set<String> entityIds );

    List<UUID> reserveIds( UUID entitySetId, int count );
}
