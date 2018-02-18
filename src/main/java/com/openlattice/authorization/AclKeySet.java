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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class AclKeySet extends HashSet<AclKey> {
    public AclKeySet() {
    }

    public AclKeySet( int initialCapacity ) {
        super( initialCapacity );
    }

    public AclKeySet( Collection<? extends AclKey> c ) {
        super( c );
    }

    public AclKeySet getValue() {
        return this;
    }

    public Set<String> getIndex() {
        return this.stream().map( AclKey::getIndex ).collect( Collectors.toSet() );
    }

}
