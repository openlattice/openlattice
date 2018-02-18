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

package com.openlattice.postgres.processors;

import com.openlattice.edm.type.EntityType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * This locks an entity type to make sure that entity set data model isn't updated while initial table setup
 * is occuring.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class CreateEntitySetTableEP extends AbstractRhizomeEntryProcessor<UUID, EntityType, Void> {
    private static final long serialVersionUID = -232768367290446715L;

    private transient HikariDataSource hds;
    private final UUID entitySetId;

    public CreateEntitySetTableEP( UUID entitySetId ) {
        this.entitySetId = entitySetId;
    }

    @Override public Void process( Entry<UUID, EntityType> entry ) {
        return null;
    }

    public void setHds( HikariDataSource hds ) {
        this.hds = hds;
    }
}
