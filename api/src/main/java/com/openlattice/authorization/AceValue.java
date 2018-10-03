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

package com.openlattice.authorization;

import static com.google.common.base.Preconditions.checkNotNull;

import com.openlattice.authorization.securable.SecurableObjectType;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class AceValue implements Set<Permission> {
    private final EnumSet<Permission> permissions;
    private       SecurableObjectType securableObjectType;
    private       OffsetDateTime      expirationDate;

    public AceValue( EnumSet<Permission> permissions, SecurableObjectType objectType ) {
        this.permissions = permissions;
        this.securableObjectType = checkNotNull( objectType, "Securable Object Type cannot be null" );
        this.expirationDate = OffsetDateTime.MAX;
    }

    public AceValue( EnumSet<Permission> permissions, SecurableObjectType objectType, OffsetDateTime expirationDate ) {
        this.permissions = permissions;
        this.securableObjectType = checkNotNull( objectType, "Securable Object Type cannot be null" );
        this.expirationDate = expirationDate;
    }

    public EnumSet<Permission> getPermissions() {
        return permissions;
    }

    public SecurableObjectType getSecurableObjectType() {
        return securableObjectType;
    }

    public OffsetDateTime getExpirationDate() {
        return expirationDate;
    }

    public void setSecurableObjectType( SecurableObjectType securableObjectType ) {
        this.securableObjectType = securableObjectType;
    }

    public void setExpirationDate( OffsetDateTime expirationDate ) {
        this.expirationDate = expirationDate;
    }

    @Override public String toString() {
        return "AceValue{" +
                "permissions=" + permissions +
                ", securableObjectType=" + securableObjectType +
                '}';
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof AceValue ) ) { return false; }

        AceValue that = (AceValue) o;

        if ( !permissions.equals( that.permissions ) ) { return false; }
        return securableObjectType == that.securableObjectType;
    }

    @Override public int hashCode() {
        int result = permissions.hashCode();
        result = 31 * result + securableObjectType.hashCode();
        return result;
    }

    @Override public boolean removeAll( Collection<?> c ) {
        return permissions.removeAll( c );
    }

    @Override public Iterator<Permission> iterator() {
        return permissions.iterator();
    }

    @Override public int size() {
        return permissions.size();
    }

    @Override public boolean isEmpty() {
        return permissions.isEmpty();
    }

    @Override public boolean contains( Object o ) {
        return permissions.contains( o );
    }

    @Override public Object[] toArray() {
        return permissions.toArray();
    }

    @Override public <T> T[] toArray( T[] a ) {
        return permissions.toArray( a );
    }

    @Override public boolean add( Permission permission ) {
        return permissions.add( permission );
    }

    @Override public boolean remove( Object o ) {
        return permissions.remove( o );
    }

    @Override public boolean containsAll( Collection<?> c ) {
        return permissions.containsAll( c );
    }

    @Override public boolean addAll( Collection<? extends Permission> c ) {
        return permissions.addAll( c );
    }

    @Override public boolean retainAll( Collection<?> c ) {
        return permissions.retainAll( c );
    }

    @Override public void clear() {
        permissions.clear();
    }

    @Override public boolean removeIf( Predicate<? super Permission> filter ) {
        return permissions.removeIf( filter );
    }

    @Override public Spliterator<Permission> spliterator() {
        return permissions.spliterator();
    }

    @Override public Stream<Permission> stream() {
        return permissions.stream();
    }

    @Override public Stream<Permission> parallelStream() {
        return permissions.parallelStream();
    }
}
