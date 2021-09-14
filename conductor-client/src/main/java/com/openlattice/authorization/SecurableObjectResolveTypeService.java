

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

import com.openlattice.authorization.securable.SecurableObjectType;

import java.util.List;
import java.util.Set;

public interface SecurableObjectResolveTypeService {
    
    void createSecurableObjectType( AclKey aclKey, SecurableObjectType type );
    
    void deleteSecurableObjectType( AclKey aclKey );
    
    SecurableObjectType getSecurableObjectType( AclKey aclKey );

    Set<AclKey> getSecurableObjectsOfType( SecurableObjectType type );

    Set<AclKey> getOrganizationExternalDbColumnAclKeys( Set<AclKey> aclKeys);
}
