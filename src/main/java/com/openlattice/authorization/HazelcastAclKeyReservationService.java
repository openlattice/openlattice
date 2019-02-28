

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

package com.openlattice.authorization;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.authorization.securable.AbstractSecurableType;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.controllers.exceptions.UniqueIdConflictException;
import com.openlattice.controllers.exceptions.TypeExistsException;
import com.openlattice.datastore.util.Util;
import com.openlattice.edm.EntitySet;
import com.openlattice.hazelcast.HazelcastMap;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

public class HazelcastAclKeyReservationService {
    public static final  String                               PRIVATE_NAMESPACE     = "_private";
    /**
     * This keeps mapping between SecurableObjectTypes that aren't associated to names and their placeholder names.
     */
    private static final EnumMap<SecurableObjectType, String> RESERVED_NAMES_AS_MAP = new EnumMap<SecurableObjectType, String>(
            SecurableObjectType.class );
    private static final Set<String>                          RESERVED_NAMES        = ImmutableSet
            .copyOf( RESERVED_NAMES_AS_MAP.values() );
    /*
     * List of name associated types.
     */
    private static final EnumSet<SecurableObjectType>         NAME_ASSOCIATED_TYPES = EnumSet
            .of( SecurableObjectType.EntityType,
                    SecurableObjectType.PropertyTypeInEntitySet,
                    SecurableObjectType.EntitySet,
                    SecurableObjectType.Principal );

    static {
        for ( SecurableObjectType objectType : SecurableObjectType.values() ) {
            if ( !NAME_ASSOCIATED_TYPES.contains( objectType ) ) {
                RESERVED_NAMES_AS_MAP.put( objectType, getPlaceholder( objectType.name() ) );
            }
        }
    }

    private final IMap<String, UUID> aclKeys;
    private final IMap<UUID, String> names;

    public HazelcastAclKeyReservationService( HazelcastInstance hazelcast ) {
        this.aclKeys = hazelcast.getMap( HazelcastMap.ACL_KEYS.name() );
        this.names = hazelcast.getMap( HazelcastMap.NAMES.name() );
    }

    public UUID getId( String name ) {
        return Util.getSafely( aclKeys, name );
    }

    public boolean isReserved( String name ) {
        return this.aclKeys.containsKey( name );
    }

    public void renameReservation( String oldName, String newName ) {
        checkArgument( !RESERVED_NAMES.contains( newName ), "Cannot rename to a reserved name" );
        checkArgument( !RESERVED_NAMES.contains( oldName ), "Cannot rename a reserved name" );

        /*
         * Attempt to associated newName with existing aclKey
         */

        final UUID associatedAclKey = checkNotNull( Util.getSafely( aclKeys, oldName ),
                "Name " + oldName + " is not being used yet." );

        final UUID existingAclKey = aclKeys.putIfAbsent( newName, associatedAclKey );

        if ( existingAclKey == null ) {
            aclKeys.delete( oldName );
            names.put( associatedAclKey, newName );
        } else {
            throw new TypeExistsException(
                    "Cannot rename " + oldName + " to existing type "
                            + newName );
        }
    }

    public void renameReservation( UUID id, FullQualifiedName newFqn ) {
        renameReservation( id, Util.fqnToString( newFqn ) );
    }

    public void renameReservation( UUID id, String newName ) {
        checkArgument( !RESERVED_NAMES.contains( newName ), "Cannot rename to a reserved name" );

        final String oldName = checkNotNull( Util.getSafely( names, id ),
                "This aclKey does not correspond to any type." );

        final UUID existingAclKey = aclKeys.putIfAbsent( newName, id );

        if ( existingAclKey == null ) {
            aclKeys.delete( oldName );
            names.put( id, newName );
        } else {
            throw new TypeExistsException(
                    "Cannot rename " + oldName + " to existing type "
                            + newName );
        }
    }

    /**
     * This function reserves a UUID for a SecurableObject based on AclKey. It throws unchecked exception
     * {@link TypeExistsException} if the type already exists or {@link UniqueIdConflictException} if a different AclKey
     * is already associated with the type.
     *
     * @param type The type for which to reserve an FQN and UUID.
     */
    public void reserveIdAndValidateType( AbstractSecurableType type ) {
        reserveIdAndValidateType( type, Suppliers.compose( Util::fqnToString, type::getType )::get );
    }

    public void reserveIdAndValidateType( EntitySet entitySet ) {
        reserveIdAndValidateType( entitySet, entitySet::getName );
    }

    /**
     * This function reserves an {@code AclKey} for a SecurableObject that has a name. It throws unchecked exceptions
     * {@link TypeExistsException} if the type already exists with the same name or {@link UniqueIdConflictException}
     * if a different AclKey is already associated with the type.
     */
    public <T extends AbstractSecurableObject> void reserveIdAndValidateType( T type, Supplier<String> namer ) {
        /*
         * Template this call and make wrappers that directly insert into type maps making fqns redundant.
         */
        final String proposedName = namer.get();
        final String currentName = names.putIfAbsent( type.getId(), proposedName );

        if ( currentName == null || proposedName.equals( currentName ) ) {
            /*
             * AclKey <-> Type association exists and is correct. Safe to try and register AclKey for type.
             */
            final UUID existingAclKey = aclKeys.putIfAbsent( proposedName, type.getId() );

            /*
             * Even if aclKey matches, letting two threads go through type creation creates potential problems when
             * entity types and entity sets are created using property types that have not quiesced. Easier for now to
             * just let one thread win and simplifies code path a lot.
             */

            if ( existingAclKey != null && !existingAclKey.equals( type.getId() ) ) {
                if ( currentName == null ) {
                    // We need to remove UUID reservation
                    names.delete( type.getId() );
                }
                throw new TypeExistsException( "Type " + proposedName + " already exists." );
            }

            /*
             * AclKey <-> Type association exists and is correct. Type <-> AclKey association exists and is correct.
             * Only a single thread should ever reach here.
             */
        } else {
            throw new UniqueIdConflictException( "AclKey is already associated with different type." );
        }
    }

    /**
     * This function reserves an id for a SecurableObject. It throws unchecked exceptions
     * {@link TypeExistsException} if the type already exists or {@link UniqueIdConflictException} if a different AclKey
     * is already associated with the type.
     */
    public void reserveId( AbstractSecurableObject type ) {
        checkArgument( RESERVED_NAMES_AS_MAP.containsKey( type.getCategory() ),
                "Unsupported securable type for reservation" );
        /*
         * Template this call and make wrappers that directly insert into type maps making fqns redundant.
         */
        final String name = names.putIfAbsent( type.getId(),
                Util.getSafely( RESERVED_NAMES_AS_MAP, type.getCategory() ) );

        /*
         * We don't care if FQN matches in this case as it provides us no additional validation information.
         */

        if ( name != null ) {
            throw new UniqueIdConflictException( "AclKey is already associated with different name." );
        }
    }

    /**
     * Releases a reserved id.
     *
     * @param id The id to release.
     */
    public void release( UUID id ) {
        String name = Util.removeSafely( names, id );

        /*
         * We always issue the delete, even if sometimes there is no aclKey registered for that FQN.
         */
        if ( name != null ) {
            aclKeys.delete( name );
        }
    }

    private static String getPlaceholder( String objName ) {
        return PRIVATE_NAMESPACE + "." + objName;
    }
}
