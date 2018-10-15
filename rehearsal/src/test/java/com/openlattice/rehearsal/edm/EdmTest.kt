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
 *
 */

package com.openlattice.rehearsal.edm

import com.openlattice.client.RetrofitFactory
import com.openlattice.edm.EdmApi
import com.openlattice.edm.type.AssociationType
import com.openlattice.rehearsal.GeneralException
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import java.util.*
import kotlin.collections.LinkedHashSet
import org.junit.rules.ExpectedException
import java.lang.reflect.UndeclaredThrowableException


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class EdmTest : MultipleAuthenticatedUsersBase() {
    companion object {
        init {
            val client = RetrofitFactory.newClient(RetrofitFactory.Environment.PRODUCTION, { "" })
            val prodEdmApi = client.create(EdmApi::class.java)
            val prodEdm = prodEdmApi.entityDataModel
        }

        @JvmStatic
        @BeforeClass
        fun init() {
            loginAs("admin")
        }
    }

    @Test
    fun testAssociationTypeCreationWrongCategory() {
        val et = MultipleAuthenticatedUsersBase.createEntityType()
        val at = AssociationType(Optional.of(et), LinkedHashSet(), LinkedHashSet(), false)

        try {
            edmApi.createAssociationType(at)
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("You cannot create an edge type with not an AssociationType category", true))
        }
    }
}