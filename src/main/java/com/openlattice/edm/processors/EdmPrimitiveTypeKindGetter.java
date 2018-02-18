

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

package com.openlattice.edm.processors;

import java.util.Map.Entry;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openlattice.edm.type.PropertyType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class EdmPrimitiveTypeKindGetter extends AbstractRhizomeEntryProcessor<UUID, PropertyType, EdmPrimitiveTypeKind> {
    private static final long            serialVersionUID      = 4485807443899509297L;
    private static final Logger          logger                = LoggerFactory
            .getLogger( EdmPrimitiveTypeKindGetter.class );
    public static final EdmPrimitiveTypeKindGetter GETTER = new EdmPrimitiveTypeKindGetter();

    @Override
    public EdmPrimitiveTypeKind process( Entry<UUID, PropertyType> entry ) {
        PropertyType pt = entry.getValue();
        if ( pt == null ) {
            logger.error( "Unable to retireve primitive type for property: {}", entry.getKey() );
            return null;
        }
        return pt.getDatatype();
    }

}
