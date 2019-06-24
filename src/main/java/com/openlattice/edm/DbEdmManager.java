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

import com.openlattice.edm.type.PropertyType;

import java.util.Collection;

/**
 * Some design assumptions:
 * <uL>
 * <li>Folks directly accessing data via SQL will be sophisticated and can deal with complexity of EAV</li>
 * <li>At scale on a live system renaming</li>
 * </uL>
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public interface DbEdmManager {

    /**
     * Instantiates the entity set structure inside the database.
     *
     * @param entitySet The entity set to instantiate.
     * @param propertyTypes The property types to instantiate.
     */
    void createEntitySet( EntitySet entitySet, Collection<PropertyType> propertyTypes ) throws Exception;

}
