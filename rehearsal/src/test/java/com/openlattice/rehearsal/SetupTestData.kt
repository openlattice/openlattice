package com.openlattice.rehearsal

import com.openlattice.edm.EntityDataModel
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import org.apache.olingo.commons.api.edm.FullQualifiedName

open class SetupTestData: MultipleAuthenticatedUsersBase() {
    companion object {
        private val OL_AUDIT_FQN = FullQualifiedName( "OPENLATTICE_AUDIT", "AUDIT" )

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
        fun importDataSet( flightFile: String, dataFile: String ) {
            val currentRetrofit = retrofitMap.get( "admin" )

        }
    }

}