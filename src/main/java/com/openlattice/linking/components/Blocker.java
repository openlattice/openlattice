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

package com.openlattice.linking.components;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import com.openlattice.linking.Entity;
import com.openlattice.linking.util.UnorderedPair;
import com.google.common.collect.SetMultimap;

/**
 * Basic Blocker interface.
 * 
 * An initialized Blocker instance should have information about the linking sets and the linking properties.
 * 
 * @author Ho Chung Siu
 *
 */
public interface Blocker {

    /**
     * Warning: We assume that the restrictions on links are enforced/validated as specified in LinkingApi. In
     * particular, only identical property types are linked on.
     * 
     * @param linkingEntitySetsWithSyncIds
     * @param linkIndexedByPropertyTypes
     * @param linkIndexedByEntitySets
     */
    public void setLinking(
            Map<UUID, UUID> linkingEntitySetsWithSyncIds,
            SetMultimap<UUID, UUID> linkIndexedByPropertyTypes,
            SetMultimap<UUID, UUID> linkIndexedByEntitySets );

    public Stream<UnorderedPair<Entity>> block();
}
