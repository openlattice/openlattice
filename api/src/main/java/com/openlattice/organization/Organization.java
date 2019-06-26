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

package com.openlattice.organization;

import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.organization.roles.Role;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.openlattice.authorization.*;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Organization {
    private final OrganizationPrincipal securablePrincipal;
    private final Set<String>        autoApprovedEmails;
    private final Set<Principal>     members;
    private final Set<Role>          roles;
    private final Set<UUID>          apps;
    private final String             phoneNumber;

    private transient int h = 0;

    @JsonCreator
    public Organization(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.PRINCIPAL ) Principal principal,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.EMAILS_FIELD ) Set<String> autoApprovedEmails,
            @JsonProperty( SerializationConstants.MEMBERS_FIELD ) Set<Principal> members,
            @JsonProperty( SerializationConstants.ROLES ) Set<Role> roles,
            @JsonProperty( SerializationConstants.APPS ) Set<UUID> apps,
            @JsonProperty( SerializationConstants.PHONE_NUMBER ) Optional<String> phoneNumber ) {
        this( new OrganizationPrincipal( id, principal, title, description ), autoApprovedEmails, members, roles, apps, phoneNumber.orElse("" ));
    }

    public Organization(
            OrganizationPrincipal securablePrincipal,
            Set<String> autoApprovedEmails,
            Set<Principal> members,
            Set<Role> roles,
            Set<UUID> apps,
            String phoneNumber ) {
        checkArgument( securablePrincipal.getPrincipalType().equals( PrincipalType.ORGANIZATION ) );
        this.securablePrincipal = securablePrincipal;
        this.autoApprovedEmails = checkNotNull( autoApprovedEmails );
        this.members = checkNotNull( members );
        this.roles = checkNotNull( roles );
        this.apps = checkNotNull( apps );
        this.phoneNumber = phoneNumber;
    }

    public Organization(
            OrganizationPrincipal principal,
            Set<String> autoApprovedEmails,
            Set<Principal> members,
            Set<Role> roles ) {
        this( principal, autoApprovedEmails, members, roles, ImmutableSet.of(), "" );
    }

    public Organization(
            Optional<UUID> id,
            Principal principal,
            String title,
            Optional<String> description,
            Set<String> autoApprovedEmails,
            Set<Principal> members,
            Set<Role> roles ) {
        this( id, principal, title, description, autoApprovedEmails, members, roles, ImmutableSet.of(), Optional.empty() );
    }

    @JsonIgnore
    public AclKey getAclKey() {
        return securablePrincipal.getAclKey();
    }

    @JsonProperty( SerializationConstants.ID_FIELD )
    public UUID getId() {
        return securablePrincipal.getId();
    }

    @JsonProperty( SerializationConstants.PRINCIPAL )
    public Principal getPrincipal() {
        return securablePrincipal.getPrincipal();
    }

    @JsonProperty( SerializationConstants.TITLE_FIELD )
    public String getTitle() {
        return securablePrincipal.getTitle();
    }

    @JsonProperty( SerializationConstants.DESCRIPTION_FIELD )
    public String getDescription() {
        return securablePrincipal.getDescription();
    }

    @JsonProperty( SerializationConstants.EMAILS_FIELD )
    public Set<String> getAutoApprovedEmails() {
        return autoApprovedEmails;
    }

    @JsonProperty( SerializationConstants.MEMBERS_FIELD )
    public Set<Principal> getMembers() {
        return members;
    }

    @JsonProperty( SerializationConstants.PHONE_NUMBER)
    public String getPhoneNumber() {
        return phoneNumber;
    }

    @JsonProperty( SerializationConstants.ROLES )
    public Set<Role> getRoles() {
        return roles;
    }

    @JsonProperty( SerializationConstants.APPS )
    public Set<UUID> getApps() {
        return apps;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        Organization that = (Organization) o;

        if ( h != that.h )
            return false;
        if ( securablePrincipal != null ?
                !securablePrincipal.equals( that.securablePrincipal ) :
                that.securablePrincipal != null )
            return false;
        if ( autoApprovedEmails != null ?
                !autoApprovedEmails.equals( that.autoApprovedEmails ) :
                that.autoApprovedEmails != null )
            return false;
        if ( members != null ? !members.equals( that.members ) : that.members != null )
            return false;
        if ( roles != null ? !roles.equals( that.roles ) : that.roles != null )
            return false;
        return apps != null ? apps.equals( that.apps ) : that.apps == null;
    }

    @Override public int hashCode() {
        int result = securablePrincipal != null ? securablePrincipal.hashCode() : 0;
        result = 31 * result + ( autoApprovedEmails != null ? autoApprovedEmails.hashCode() : 0 );
        result = 31 * result + ( members != null ? members.hashCode() : 0 );
        result = 31 * result + ( roles != null ? roles.hashCode() : 0 );
        result = 31 * result + ( apps != null ? apps.hashCode() : 0 );
        result = 31 * result + h;
        return result;
    }

    @Override public String toString() {
        return "Organization{" +
                "securablePrincipal=" + securablePrincipal +
                ", autoApprovedEmails=" + autoApprovedEmails +
                ", members=" + members +
                ", roles=" + roles +
                ", apps=" + apps +
                '}';
    }

    public OrganizationPrincipal getSecurablePrincipal() {
        return securablePrincipal;
    }

}
