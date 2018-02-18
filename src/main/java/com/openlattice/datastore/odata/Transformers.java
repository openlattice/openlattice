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

package com.openlattice.datastore.odata;

import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlPropertyRef;

import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.EntityType;
import com.openlattice.datastore.services.EdmManager;

public final class Transformers {
    private Transformers() {}

    public static final class EntityTypeTransformer {
        private final EdmManager dms;

        public EntityTypeTransformer( EdmManager dms ) {
            this.dms = dms;
        }

        public CsdlEntityType transform( EntityType objectType ) {
            if ( objectType == null ) {
                return null;
            }

            CsdlEntityType entityType = new CsdlEntityType();

            entityType.setName( objectType.getType().getName() );

            entityType.setKey( objectType.getKey().stream()
                    .map( dms::getPropertyType )
                    .map( k -> new CsdlPropertyRef().setName( k.getType().getName() ) )
                    .collect( Collectors.toList() ) );
            entityType.setProperties(
                    objectType.getProperties().stream()
                            .map( dms::getPropertyType )
                            .map( ( prop ) -> new CsdlProperty().setName( prop.getType().getName() )
                                    .setType( prop.getType() ) )
                            .collect( Collectors.toList() ) );
            return entityType;
        }

    }

    public static final class EntitySetTransformer {
        private final EdmManager dms;
        
        public EntitySetTransformer ( EdmManager dms ){
            this.dms = dms;
        }
        
        public CsdlEntitySet transform( EntitySet entitySet ){
            if ( entitySet == null ) {
                return null;
            }

            return new CsdlEntitySet()
                    .setType( dms.getEntityTypeFqn( entitySet.getEntityTypeId() ) )
                    .setName( entitySet.getName() );
        }
    }
}
