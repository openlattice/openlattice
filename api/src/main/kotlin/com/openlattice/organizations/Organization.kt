package com.openlattice.organizations

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.notifications.sms.SmsEntitySetInformation
import com.openlattice.organization.OrganizationPrincipal
import com.openlattice.organization.roles.Role
import java.util.*

/**
 * Organization data object
 * These are mutable for convenience, but the only way to persistently mutate is through entry processor.
 * If you mutate these or pass around values do so at your own risk.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

@JsonIgnoreProperties(value = ["metadataEntitySetIds"])
data class Organization @JvmOverloads constructor(
    var securablePrincipal: OrganizationPrincipal,
    var adminRoleAclKey: AclKey = AclKey(securablePrincipal.id, UUID.randomUUID()),
    val emailDomains: MutableSet<String>,
    val members: MutableSet<Principal>,
    val roles: MutableSet<Role>,
    val smsEntitySetInfo: MutableSet<SmsEntitySetInformation>,
    val partitions: MutableList<Int> = mutableListOf(),
    val apps: MutableSet<UUID> = mutableSetOf(),
    val connections: MutableSet<String> = mutableSetOf(),
    val grants: MutableMap<UUID, MutableMap<GrantType, Grant>> = mutableMapOf()
) {

    val id: UUID
        @JsonProperty(SerializationConstants.ID_FIELD)
        get() = securablePrincipal.id

    val principal: Principal
        @JsonProperty(SerializationConstants.PRINCIPAL)
        get() = securablePrincipal.principal

    val title: String
        @JsonProperty(SerializationConstants.TITLE_FIELD)
        get() = securablePrincipal.title

    val description: String
        @JsonProperty(SerializationConstants.DESCRIPTION_FIELD)
        get() = securablePrincipal.description


    @JsonCreator
    constructor(
        @JsonProperty(SerializationConstants.ID_FIELD) id: Optional<UUID> = Optional.of(UUID.randomUUID()),
        @JsonProperty(SerializationConstants.ADMIN_ROLE_ACL_KEY) adminRoleAclKey: Optional<AclKey>,
        @JsonProperty(SerializationConstants.PRINCIPAL) principal: Principal,
        @JsonProperty(SerializationConstants.TITLE_FIELD) title: String,
        @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) description: Optional<String>,
        @JsonProperty(SerializationConstants.EMAIL_DOMAINS) emailDomains: MutableSet<String> = mutableSetOf(),
        @JsonProperty(SerializationConstants.MEMBERS_FIELD) members: MutableSet<Principal>,
        @JsonProperty(SerializationConstants.ROLES) roles: MutableSet<Role>,
        @JsonProperty(SerializationConstants.APPS) apps: MutableSet<UUID>,
        @JsonProperty(SerializationConstants.SMS_ENTITY_SET_INFO) smsEntitySetInfo: Optional<MutableSet<SmsEntitySetInformation>>,
        @JsonProperty(SerializationConstants.PARTITIONS) partitions: Optional<MutableList<Int>>,
        @JsonProperty(SerializationConstants.CONNECTIONS) connections: MutableSet<String> = mutableSetOf(),
        @JsonProperty(SerializationConstants.GRANTS) grants: MutableMap<UUID, MutableMap<GrantType, Grant>> = mutableMapOf(),
    ) : this(
        securablePrincipal = OrganizationPrincipal(id, principal, title, description),
        adminRoleAclKey = adminRoleAclKey.orElse(AclKey(id.get(), UUID.randomUUID())),
        emailDomains = emailDomains,
        members = members,
        roles = roles,
        smsEntitySetInfo = smsEntitySetInfo.orElse(mutableSetOf<SmsEntitySetInformation>()),
        partitions = partitions.orElse(mutableListOf()),
        apps = apps,
        connections = connections,
        grants = grants,
    )

    init {
        check(securablePrincipal.principalType == PrincipalType.ORGANIZATION) { "Organization principal must be of PrincipalType.ORGANIZATION" }
    }

    @JsonIgnore
    fun getAclKey(): AclKey {
        return securablePrincipal.aclKey
    }
}
