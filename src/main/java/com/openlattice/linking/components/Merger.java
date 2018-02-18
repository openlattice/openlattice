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

import java.util.Set;
import java.util.UUID;

import com.openlattice.data.EntityKey;
import com.google.common.collect.SetMultimap;

/**
 * Basic Merger interface. 
 * 
 * An initialized Merger instance should have information about the linking sets and the linking properties. 
 * The Merger instance should also compute the set of authorized properties in each involved entity set after linking. If there is no authorized properties in the overall linked entity set, throw an exception.
 * @author Ho Chung Siu
 *
 */
//TODO Maybe worthwhile to move initialization of Merger to the beginning of linking function. If the set of authorized property types in the overall linked entity set is empty, throw an exception to stop the linking.
public interface Merger {

    public Iterable<SetMultimap<UUID, Object>> merge( Set<EntityKey> linkingEntities );
}
