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

package com.openlattice.edm;

import static com.google.common.base.Preconditions.checkState;

import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A directed graph of types representing a template for instantiating an entity set collection.
 * This class is not currently thread safe.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EdmTemplate extends AbstractSecurableObject {
    private static final Logger logger = LoggerFactory.getLogger( EdmTemplate.class );

    private final String                         name;
    private final Set<EdmTemplate>               edmTemplates;
    private final Map<String, UUID>              entities;
    private final Map<String, Map<String, UUID>> associations;

    @JsonCreator
    public EdmTemplate(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.NAME_FIELD ) String name,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.EDM_TEMPLATES_FIELD ) Set<EdmTemplate> edmTemplates,
            @JsonProperty( SerializationConstants.ENTITIES ) Map<String, UUID> entities,
            @JsonProperty( SerializationConstants.ASSOCIATIONS )
                    Map<String, Map<String, UUID>> associations ) {
        super( id, title, description );
        this.name = name;
        this.edmTemplates = edmTemplates;
        this.entities = entities;
        this.associations = associations;
    }

    public EdmTemplate(
            UUID id,
            String name,
            String title,
            Optional<String> description,
            Map<String, Map<String, UUID>> associations,
            Set<EdmTemplate> edmTemplates, Map<String, UUID> entities ) {
        this( Optional.of( id ), name, title, description, edmTemplates, entities, associations );
    }

    public void mergeTemplate( EdmTemplate edmTemplate ) {
        checkState( isCompatible( edmTemplate ), "Template is not compatible." );
        edmTemplates.add( edmTemplate );
        entities.putAll( edmTemplate.getEntities() );

        final Map<String, Map<String, UUID>> otherAssociations = edmTemplate.getAssociations();

        for ( Entry<String, Map<String, UUID>> srcEntry : otherAssociations.entrySet() ) {
            final Map<String, UUID> srcEntryMap = srcEntry.getValue();
            final Map<String, UUID> dsts = associations.putIfAbsent( srcEntry.getKey(), srcEntryMap );
            if ( dsts != null ) {
                dsts.putAll( srcEntryMap );
            }
        }
        //TODO: Merge entity types.
        //Map<String, UUID> entityTypesToMerge = edmTemplate.getEntities();
    }

    public boolean isCompatible( EdmTemplate edmTemplate ) {
        if ( !isCompatible( entities, edmTemplate.getEntities() ) ) {
            return false;
        }

        final Map<String, Map<String, UUID>> otherAssociations = edmTemplate.getAssociations();
        final Set<String> overlaps = Sets.intersection( otherAssociations.keySet(), associations.keySet() );

        for ( String overlap : overlaps ) {
            if ( !isCompatible( associations.get( overlap ), otherAssociations.get( overlap ) ) ) {
                return false;
            }
        }
        return true;
    }

    public void removeTemplate( EdmTemplate edmTemplate ) {
        //If template was present and removed recompute members.
        if ( edmTemplates.remove( edmTemplate ) ) {
            entities.clear();
            associations.clear();
            edmTemplates.forEach( this::mergeTemplate );
        }
    }

    public Map<String, UUID> getEntities() {
        return entities;
    }

    public Map<String, Map<String, UUID>> getAssociations() {
        return associations;
    }

    @Override public SecurableObjectType getCategory() {
        return SecurableObjectType.EdmTemplate;
    }

    private static <V> boolean isCompatible( Map<String, V> dst, Map<String, V> src ) {
        Set<String> overlaps = Sets.intersection( src.keySet(), dst.keySet() );
        for ( String overlap : overlaps ) {
            V dstValue = dst.get( overlap );
            V srcValue = src.get( overlap );
            if ( !dstValue.equals( srcValue ) ) {
                logger.debug( "Incompatible values {} does not match expected value {} for key {}.",
                        srcValue,
                        dstValue,
                        overlap );
                return false;
            }
        }

        return true;
    }
}
