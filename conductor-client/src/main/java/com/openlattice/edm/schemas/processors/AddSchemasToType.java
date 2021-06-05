

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

package com.openlattice.edm.schemas.processors;

import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.openlattice.authorization.securable.AbstractSchemaAssociatedSecurableType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;


/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class AddSchemasToType<T extends  AbstractSchemaAssociatedSecurableType> extends AbstractRhizomeEntryProcessor<UUID, T, Void> {
    private static final long                          serialVersionUID = 7905367675743576380L;
    private final        Collection<FullQualifiedName> schemas;

    public AddSchemasToType( Collection<FullQualifiedName> schemas ) {
        this.schemas = Preconditions.checkNotNull( schemas );
    }

    @Override
    public Void process( Entry<UUID, T> entry ) {
        T securableType = entry.getValue();
        if ( securableType != null ) {
            securableType.addToSchemas( schemas );
            entry.setValue( securableType );
        }
        return null;
    }

    public Collection<FullQualifiedName> getSchemas() {
        return schemas;
    }

}
