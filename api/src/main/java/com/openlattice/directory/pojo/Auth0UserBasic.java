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

package com.openlattice.directory.pojo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Sets;

@JsonIgnoreProperties(
    ignoreUnknown = true )
public class Auth0UserBasic {
    public static final String USER_ID_FIELD       = "user_id";
    public static final String EMAIL_FIELD         = "email";
    public static final String NICKNAME_FIELD      = "nickname";
    public static final String USERNAME_FIELD      = "username";
    public static final String APP_METADATA_FIELD  = "app_metadata";
    public static final String ROLES_FIELD         = "roles";
    public static final String ORGANIZATIONS_FIELD = "organization";

    private final String       userId;
    private final String       email;
    private final String       nickname;
    private final String       username;
    private final Set<String>  roles;
    private final Set<String>  organizations;
    private final OffsetDateTime loadTime = OffsetDateTime.now();

    @SuppressWarnings( "unchecked" )
    @JsonCreator
    @JsonIgnoreProperties(
        ignoreUnknown = true )
    public Auth0UserBasic(
            @JsonProperty( USER_ID_FIELD ) String userId,
            @JsonProperty( EMAIL_FIELD ) String email,
            @JsonProperty( NICKNAME_FIELD ) String nickname,
            @JsonProperty( APP_METADATA_FIELD ) Map<String, Object> appMetadata ) {
        this.userId = userId;
        this.email = email;
        this.nickname = nickname;
        if ( this.email != null ) {
            this.username = this.email;
        } else if ( this.nickname != null ) {
            this.username = this.nickname;
        } else {
            this.username = this.userId;
        }
        this.roles = Sets.newHashSet(
                ( (List<String>) MoreObjects.firstNonNull( appMetadata, new HashMap<>() ).getOrDefault( "roles",
                        new ArrayList<String>() ) ) );
        this.organizations = Sets
                .newHashSet( ( (List<String>) MoreObjects.firstNonNull( appMetadata, new HashMap<>() ).getOrDefault(
                        "organizations",
                        new ArrayList<String>() ) ) );
    }

    public Auth0UserBasic( String userId, String email, String nickname, Set<String> roles, Set<String> organizations ) {
        this.userId = userId;
        this.email = email;
        this.nickname = nickname;
        if ( this.email != null ) {
            this.username = this.email;
        } else if ( this.nickname != null ) {
            this.username = this.nickname;
        } else {
            this.username = this.userId;
        }
        this.roles = roles;
        this.organizations = organizations;
    }

    @JsonProperty( USER_ID_FIELD )
    public String getUserId() {
        return userId;
    }

    @JsonProperty( EMAIL_FIELD )
    public String getEmail() {
        return email;
    }

    @JsonProperty( NICKNAME_FIELD )
    public String getNickname() {
        return nickname;
    }

    @JsonProperty( USERNAME_FIELD )
    public String getUsername() {
        return username;
    }

    @JsonProperty( ROLES_FIELD )
    public Set<String> getRoles() {
        return Collections.unmodifiableSet( roles );
    }

    @JsonProperty( ORGANIZATIONS_FIELD )
    public Set<String> getOrganizations() {
        return Collections.unmodifiableSet( organizations );
    }

    @JsonIgnore
    public OffsetDateTime getLoadTime() {
        return loadTime;
    }

    @Override
    public String toString() {
        return "Auth0UserBasic{" +
                "userId='" + userId + '\'' +
                ", email='" + email + '\'' +
                ", nickname='" + nickname + '\'' +
                ", username='" + username + '\'' +
                ", roles=" + roles +
                ", organizations=" + organizations +
                '}';
    }

    @Override
    public int hashCode() {
        // user id is non-null and uniquely identifies the user.
        return userId.hashCode();
    }

    @Override
    public boolean equals( Object obj ) {
        // user id is a unique identifier.
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        Auth0UserBasic other = (Auth0UserBasic) obj;
        if ( userId == null ) {
            if ( other.userId != null ) return false;
        } else if ( !userId.equals( other.userId ) ) return false;
        return true;
    }

}
