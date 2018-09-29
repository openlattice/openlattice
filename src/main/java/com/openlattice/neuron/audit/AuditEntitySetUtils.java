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
 */

package com.openlattice.neuron.audit;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.neuron.signals.Signal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditEntitySetUtils {

    // TODO: where does this belong?
    public static final Principal OPEN_LATTICE_PRINCIPAL = new Principal( PrincipalType.USER, "OpenLattice" );
    private static final Logger logger = LoggerFactory.getLogger( AuditEntitySetUtils.class );
    public static final String AUDIT_ENTITY_SET_NAME       = "OpenLattice Audit Entity Set";
    private static final String OPENLATTICE_AUDIT_NAMESPACE = "OPENLATTICE_AUDIT";
    public static final FullQualifiedName DETAILS_PT_FQN = new FullQualifiedName( OPENLATTICE_AUDIT_NAMESPACE,
            "DETAILS" );
    public static final FullQualifiedName TYPE_PT_FQN    = new FullQualifiedName( OPENLATTICE_AUDIT_NAMESPACE,
            "TYPE" );
    public static final FullQualifiedName AUDIT_ET_FQN   = new FullQualifiedName( OPENLATTICE_AUDIT_NAMESPACE,
            "AUDIT" );
    private static Collection<PropertyType> PROPERTIES;
    private static PropertyType             TYPE_PROPERTY_TYPE;
    private static PropertyType             DETAILS_PROPERTY_TYPE;
    private static EntityType               AUDIT_ENTITY_TYPE;
    private static EntitySet                AUDIT_ENTITY_SET;
    private static UUID                     AUDIT_ENTITY_SET_SYNC_ID;

    // @formatter:off
    public AuditEntitySetUtils() {}
    // @formatter:on

    // PlasmaCoupling magic
    public static void initialize( EdmManager entityDataModelManager ) {

        EntitySet maybeAuditEntitySet = entityDataModelManager.getEntitySet( AUDIT_ENTITY_SET_NAME );

        if ( maybeAuditEntitySet == null ) {
            initializePropertyTypes( entityDataModelManager );
            initializeEntityType( entityDataModelManager );
            initializeEntitySet( entityDataModelManager );
        } else {
            AUDIT_ENTITY_SET = maybeAuditEntitySet;
            AUDIT_ENTITY_TYPE = entityDataModelManager.getEntityType( AUDIT_ENTITY_SET.getEntityTypeId() );
            TYPE_PROPERTY_TYPE = entityDataModelManager.getPropertyType( TYPE_PT_FQN );
            DETAILS_PROPERTY_TYPE = entityDataModelManager.getPropertyType( DETAILS_PT_FQN );
            PROPERTIES = entityDataModelManager.getPropertyTypes( AUDIT_ENTITY_TYPE.getProperties() );
        }
    }

    private static void initializePropertyTypes( EdmManager entityDataModelManager ) {

        // TYPE_PROPERTY_TYPE
        try {
            TYPE_PROPERTY_TYPE = entityDataModelManager.getPropertyType( TYPE_PT_FQN );
        } catch ( NullPointerException e ) {
            TYPE_PROPERTY_TYPE = null;
        }

        if ( TYPE_PROPERTY_TYPE == null ) {
            TYPE_PROPERTY_TYPE = new PropertyType(
                    TYPE_PT_FQN,
                    "Type",
                    Optional.of( "The type of event being logged." ),
                    ImmutableSet.of(),
                    EdmPrimitiveTypeKind.String
            );
            entityDataModelManager.createPropertyTypeIfNotExists( TYPE_PROPERTY_TYPE );
        }

        // DETAILS_PROPERTY_TYPE
        try {
            DETAILS_PROPERTY_TYPE = entityDataModelManager.getPropertyType( DETAILS_PT_FQN );
        } catch ( NullPointerException e ) {
            DETAILS_PROPERTY_TYPE = null;
        }

        if ( DETAILS_PROPERTY_TYPE == null ) {
            DETAILS_PROPERTY_TYPE = new PropertyType(
                    DETAILS_PT_FQN,
                    "Details",
                    Optional.of( "Any details about the event being logged." ),
                    ImmutableSet.of(),
                    EdmPrimitiveTypeKind.String
            );
            entityDataModelManager.createPropertyTypeIfNotExists( DETAILS_PROPERTY_TYPE );
        }

        PROPERTIES = ImmutableList.of(
                TYPE_PROPERTY_TYPE,
                DETAILS_PROPERTY_TYPE
        );
    }

    private static void initializeEntityType( EdmManager entityDataModelManager ) {

        try {
            AUDIT_ENTITY_TYPE = entityDataModelManager.getEntityType( AUDIT_ET_FQN );
        } catch ( NullPointerException e ) {
            AUDIT_ENTITY_TYPE = null;
        }

        if ( AUDIT_ENTITY_TYPE == null ) {
            AUDIT_ENTITY_TYPE = new EntityType(
                    AUDIT_ET_FQN,
                    "OpenLattice Audit",
                    "The OpenLattice Audit Entity Type.",
                    ImmutableSet.of(),
                    Sets.newLinkedHashSet(
                            Sets.newHashSet( TYPE_PROPERTY_TYPE.getId() )
                    ),
                    Sets.newLinkedHashSet(
                            Sets.newHashSet( TYPE_PROPERTY_TYPE.getId(), DETAILS_PROPERTY_TYPE.getId() )
                    ),
                    LinkedHashMultimap.create(),
                    Optional.empty(),
                    Optional.empty()
            );
            entityDataModelManager.createEntityType( AUDIT_ENTITY_TYPE );
        }
    }

    private static void initializeEntitySet( EdmManager entityDataModelManager ) {

        try {
            AUDIT_ENTITY_SET = entityDataModelManager.getEntitySet( AUDIT_ENTITY_SET_NAME );
        } catch ( NullPointerException e ) {
            AUDIT_ENTITY_SET = null;
        }

        if ( AUDIT_ENTITY_SET == null ) {
            AUDIT_ENTITY_SET = new EntitySet(
                    AUDIT_ENTITY_TYPE.getId(),
                    AUDIT_ENTITY_SET_NAME,
                    AUDIT_ENTITY_SET_NAME,
                    Optional.of( AUDIT_ENTITY_SET_NAME ),
                    ImmutableSet.of( "support@openlattice.com" )
            );
            entityDataModelManager.createEntitySet( OPEN_LATTICE_PRINCIPAL, AUDIT_ENTITY_SET );
        }
    }

    public static UUID getId() {

        return AUDIT_ENTITY_SET.getId();
    }

    public static UUID getSyncId() {

        return AUDIT_ENTITY_SET_SYNC_ID;
    }

    public static void setSyncId( UUID syncId ) {

        AUDIT_ENTITY_SET_SYNC_ID = syncId;
    }

    public static Map<UUID, PropertyType> getPropertyDataTypesMap() {

        return PROPERTIES
                .stream()
                .collect(
                        Collectors.toMap( PropertyType::getId, Function.identity() )
                );
    }

    public static List<SetMultimap<UUID, Object>> prepareAuditEntityData( Signal signal, String entityId ) {
        SetMultimap<UUID, Object> propertyValuesMap = HashMultimap.create();
        propertyValuesMap.put( DETAILS_PROPERTY_TYPE.getId(), signal.getDetails() );
        propertyValuesMap.put( TYPE_PROPERTY_TYPE.getId(), signal.getType().name() );

        return ImmutableList.of( propertyValuesMap );
    }
}
