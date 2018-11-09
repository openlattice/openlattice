package com.openlattice.rehearsal

import com.openlattice.authentication.AuthenticationTest
import com.openlattice.edm.EntityDataModel
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import com.openlattice.shuttle.ShuttleCli
import com.openlattice.shuttle.main
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.io.File



open class SetupTestData: MultipleAuthenticatedUsersBase() {
    companion object {
        private val OL_AUDIT_FQN = FullQualifiedName( "OPENLATTICE_AUDIT", "AUDIT" )
        private const val DATA_FOLDER = "data"
        private const val FLIGHT_FOLDER = "flights"

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

            main(arrayOf(
                    "-${ShuttleCli.FLIGHT}=$flightFile",
                    "-${ShuttleCli.CSV}=$dataFile",
                    "-${ShuttleCli.ENVIRONMENT}=LOCAL",
                    "-${ShuttleCli.TOKEN}=$tokenAdmin",
                    "-${ShuttleCli.CREATE}"))
        }
    }

}