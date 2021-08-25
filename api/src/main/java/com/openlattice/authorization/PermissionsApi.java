/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.authorization;

import retrofit2.http.*;

import java.util.*;

/**
 * @author Ho Chung Siu
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public interface PermissionsApi {

    String SERVICE    = "/datastore";
    String CONTROLLER = "/permissions";
    String BASE       = SERVICE + CONTROLLER;

    String BULK    = "/bulk";
    String EXPLAIN = "/explain";
    String UPDATE  = "/update";

    /**
     * Adds, removes, or sets the ace for a particular acl key. Successful only if user is the owner of acl key.
     * <p>
     * If the action is ADD, all specified permissions will be merged into any existing sets of permissions for the given users.
     * If the action is SET, all specified permissions will replace any existing sets of permissions for the given users.
     * If the action is REMOVE, all specified permissions will be removed from any existing sets of permissions for the given users.
     * </p>
     *
     * @param req The acl key, the principals, and the aces to set for that particular ace key.
     */
    @PATCH( BASE )
    Void updateAcl( @Body AclData req );

    /**
     * Adds, removes, or sets the ace for a particular set of acl keys. Successful only if user is the owner of all acl keys.
     * <p>
     * If the action is ADD, all specified permissions will be merged into any existing sets of permissions for the given users.
     * If the action is SET, all specified permissions will replace any existing sets of permissions for the given users.
     * If the action is REMOVE, all specified permissions will be removed from any existing sets of permissions for the given users.
     * </p>
     *
     * @param req The acl key, the principals, and the aces to set for that particular ace key.
     * @return Void
     */
    @PATCH( BASE + UPDATE )
    Void updateAcls( @Body List<AclData> req );

    /**
     * Retrieves the acl for a particular acl key. Only return if user is the owner of acl key.
     *
     * @param aclKey The acl key.
     */
    @POST( BASE )
    Acl getAcl( @Body AclKey aclKey );

    /**
     * Retrieves the set of Acls for the given set of AclKeys, only if the user is OWNER of all AclKeys.
     *
     * @param aclKeys the set of AclKeys to operate on
     * @return the set of Acls corresponding to the given set of AclKeys
     */
    @POST( BASE + BULK )
    Set<Acl> getAcls( @Body Set<AclKey> aclKeys );

    /**
     * Retrieves the acl for a particular acl key, with explanation of where the permissions come from. Only return if
     * user is the owner of acl key.
     *
     * @param aclKey The acl key.
     * @return The aces for the requested acl key, together with the explanation.
     */
    @POST( BASE + EXPLAIN )
    Collection<AclExplanation> getAclExplanation( @Body AclKey aclKey );

}
