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

package com.openlattice.rehearsal

import com.openlattice.authentication.AuthenticationTest
import com.openlattice.edm.EntityDataModel
import com.openlattice.organizations.PrincipalSet
import com.openlattice.organizations.mapstores.PrincipalSetMapstore
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import com.openlattice.shuttle.ShuttleCli
import com.openlattice.shuttle.main
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.io.File

/**
 * Helper functions for setting up data model and integrating data before running tests.
 */
open class SetupTestData: MultipleAuthenticatedUsersBase() {
    companion object {
        private val OL_AUDIT_FQN = FullQualifiedName( "OPENLATTICE_AUDIT", "AUDIT" )
        private const val DATA_FOLDER = "data"
        private const val FLIGHT_FOLDER = "flights"

        /**
         * Import EDM from production environment. Note: It assumes that entity data model is empty and that properties
         * being created don't already exist
         */
        @JvmStatic
        fun initEdm() {
            loginAs( "prod" )
            val prodEdm = removeAuditType( edmApi.entityDataModel )

            loginAs( "admin" )
            // update version number
            val localVersion = edmApi.entityDataModelVersion

            val edm = EntityDataModel(
                    localVersion,
                    prodEdm.namespaces,
                    prodEdm.schemas,
                    prodEdm.entityTypes,
                    prodEdm.associationTypes,
                    prodEdm.propertyTypes )

            edmApi.updateEntityDataModel( edm )
        }

        /**
         * Filter out audit entity sets
         */
        @JvmStatic
        fun removeAuditType(edm: EntityDataModel): EntityDataModel {
            val propertyTypes = edm.propertyTypes.filter { it.type.toString() != OL_AUDIT_FQN.toString() }
            val entityTypes = edm.entityTypes.filter{ it.type.toString() != OL_AUDIT_FQN.toString() }
            val associationTypes = edm.associationTypes.filter {
                it.associationEntityType.type.toString() != OL_AUDIT_FQN.toString()
            }

            return EntityDataModel(
                    edm.version,
                    edm.namespaces,
                    edm.schemas,
                    entityTypes,
                    associationTypes,
                    propertyTypes )
        }

        /**
         * Import datasets via Shuttle
         */
        @JvmStatic
        fun importDataSet( flightFileName: String, dataFileName: String ) {
            loginAs( "admin" )
            val tokenAdmin = AuthenticationTest.getAuthentication(authOptions).credentials

            val flightFile = File(Thread.currentThread().contextClassLoader.getResource(FLIGHT_FOLDER).file,
                    flightFileName).absolutePath
            val dataFile = File(Thread.currentThread().contextClassLoader.getResource(DATA_FOLDER).file, dataFileName)
                    .absolutePath
            val email = getUserInfo( SetupEnvironment.admin ).email

            main(arrayOf(
                    "-${ShuttleCli.FLIGHT}=$flightFile",
                    "-${ShuttleCli.CSV}=$dataFile",
                    "-${ShuttleCli.ENVIRONMENT}=LOCAL",
                    "-${ShuttleCli.TOKEN}=$tokenAdmin",
                    "-${ShuttleCli.CREATE}=$email"))
        }
    }

}