/*
 * Copyright (C) 2019. OpenLattice, Inc.
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
 *
 */
package com.openlattice.authorization

import com.google.common.eventbus.EventBus
import com.openlattice.auditing.AuditingConfiguration
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EdmService
import com.openlattice.datastore.services.EntitySetService
import com.openlattice.edm.PostgresEdmManager
import com.openlattice.edm.properties.PostgresTypeManager
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager
import com.openlattice.edm.schemas.postgres.PostgresSchemaQueryService
import com.openlattice.mapstores.TestDataFactory
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.junit.Assert
import org.junit.Test
import org.mockito.Mockito
import java.util.*

class EdmAuthorizationHelperTest : HzAuthzTest() {
    private val edmAuthHelper: EdmAuthorizationHelper

    init {
        val auditingConfig = testServer.context.getBean(AuditingConfiguration::class.java)

        val edmManager = EdmService(
                hazelcastInstance,
                HazelcastAclKeyReservationService(hazelcastInstance),
                hzAuthz,
                PostgresEdmManager(hds, hazelcastInstance),
                PostgresTypeManager(hds),
                HazelcastSchemaManager(hazelcastInstance, PostgresSchemaQueryService(hds))
        )
        val entitySetManager = EntitySetService(
                hazelcastInstance,
                Mockito.mock(EventBus::class.java),
                PostgresEdmManager(hds, hazelcastInstance),
                HazelcastAclKeyReservationService(hazelcastInstance),
                hzAuthz,
                PartitionManager(hazelcastInstance, hds),
                edmManager,
                hds,
                auditingConfig
        )

        edmAuthHelper = EdmAuthorizationHelper(edmManager, hzAuthz, entitySetManager)
    }

    /**
     * Testcase for [EdmAuthorizationHelper.getAuthorizedPropertyTypesOfNormalEntitySet]
     */
    @Test
    fun testGetAuthorizedPropertyTypesOfNormalEntitySet() {
        val propertyType1 = TestDataFactory.propertyType()
        val propertyType2 = TestDataFactory.propertyType()
        val entityType = TestDataFactory.childEntityTypeWithPropertyType(
                null,
                Optional.empty<FullQualifiedName>(),
                setOf(propertyType1.id, propertyType2.id),
                null,
                propertyType1,
                propertyType2
        )
        val entitySet = TestDataFactory.entitySetWithType(entityType.id)

        val principal1 = TestDataFactory.userPrincipal()
        val principal2 = TestDataFactory.userPrincipal()

        val property1Acl = AclKey(entitySet.id, propertyType1.id)
        hzAuthz.setSecurableObjectType(property1Acl, SecurableObjectType.PropertyTypeInEntitySet)
        val property2Acl = AclKey(entitySet.id, propertyType2.id)
        hzAuthz.setSecurableObjectType(property2Acl, SecurableObjectType.PropertyTypeInEntitySet)

        val properties = mapOf(propertyType1.id to propertyType1, propertyType2.id to propertyType2)


        // no authorization has been given
        val authorizedProperties1 = edmAuthHelper.getAuthorizedPropertyTypesOfNormalEntitySet(
                entitySet.id,
                properties,
                EdmAuthorizationHelper.READ_PERMISSION,
                setOf(principal1, principal2)
        )

        Assert.assertEquals(0, authorizedProperties1.size)
        Assert.assertEquals(2, properties.size) // did not modify original properties


        // add principal 1 read on 1 property
        hzAuthz.addPermission(property1Acl, principal1, EdmAuthorizationHelper.READ_PERMISSION)

        val authorizedProperties2 = edmAuthHelper.getAuthorizedPropertyTypesOfNormalEntitySet(
                entitySet.id,
                properties,
                EdmAuthorizationHelper.READ_PERMISSION,
                setOf(principal1, principal2)
        )

        Assert.assertEquals(1, authorizedProperties2.size) // at least 1 principal has read
        Assert.assertEquals(propertyType1, authorizedProperties2[propertyType1.id])


        // add principal 2 read on 1 property
        hzAuthz.addPermission(property1Acl, principal2, EdmAuthorizationHelper.READ_PERMISSION)

        val authorizedProperties3 = edmAuthHelper.getAuthorizedPropertyTypesOfNormalEntitySet(
                entitySet.id,
                properties,
                EdmAuthorizationHelper.READ_PERMISSION,
                setOf(principal1, principal2)
        )

        Assert.assertEquals(1, authorizedProperties3.size)
        Assert.assertEquals(propertyType1, authorizedProperties3[propertyType1.id])


        // add principal 1 read on all properties
        hzAuthz.addPermission(property2Acl, principal1, EdmAuthorizationHelper.READ_PERMISSION)

        val authorizedProperties4 = edmAuthHelper.getAuthorizedPropertyTypesOfNormalEntitySet(
                entitySet.id,
                properties,
                EdmAuthorizationHelper.READ_PERMISSION,
                setOf(principal1, principal2)
        )

        Assert.assertEquals(2, authorizedProperties4.size) // at least 1 principal has read on both
        Assert.assertEquals(propertyType1, authorizedProperties4[propertyType1.id])
        Assert.assertEquals(propertyType2, authorizedProperties4[propertyType2.id])


        //add principal 2 read on all properties
        hzAuthz.addPermission(property2Acl, principal2, EdmAuthorizationHelper.READ_PERMISSION)

        val authorizedProperties5 = edmAuthHelper.getAuthorizedPropertyTypesOfNormalEntitySet(
                entitySet.id,
                properties,
                EdmAuthorizationHelper.READ_PERMISSION,
                setOf(principal1, principal2)
        )

        Assert.assertEquals(2, authorizedProperties5.size)
        Assert.assertEquals(propertyType1, authorizedProperties5[propertyType1.id])
        Assert.assertEquals(propertyType2, authorizedProperties5[propertyType2.id])


        // try to get authorized properties for both read and write
        val authorizedProperties6 = edmAuthHelper.getAuthorizedPropertyTypesOfNormalEntitySet(
                entitySet.id,
                properties,
                EnumSet.of(Permission.READ, Permission.WRITE),
                setOf(principal1, principal2)
        )

        Assert.assertEquals(0, authorizedProperties6.size)

        // add principal 1 write on 1 property
        hzAuthz.addPermission(property1Acl, principal1, EdmAuthorizationHelper.WRITE_PERMISSION)

        val authorizedProperties7 = edmAuthHelper.getAuthorizedPropertyTypesOfNormalEntitySet(
                entitySet.id,
                properties,
                EnumSet.of(Permission.READ, Permission.WRITE),
                setOf(principal1, principal2)
        )

        Assert.assertEquals(1, authorizedProperties7.size)
        Assert.assertEquals(propertyType1, authorizedProperties7[propertyType1.id])
    }
}