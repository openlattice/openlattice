package com.openlattice.edm.tasks

import com.hazelcast.core.HazelcastInstance
import com.openlattice.apps.AppApi
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.client.RetrofitFactory
import com.openlattice.collections.CollectionsApi
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.EdmApi
import com.openlattice.edm.EntityDataModel
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.PostConstructInitializerTaskDependencies
import com.openlattice.tasks.Task
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory

class EdmSyncInitializerTask : HazelcastInitializationTask<EdmSyncInitializerDependencies> {

    companion object {
        private val OL_AUDIT_FQN = FullQualifiedName("OPENLATTICE_AUDIT", "AUDIT")
        private val logger = LoggerFactory.getLogger(EdmSyncInitializerTask::class.java)
    }

    override fun getInitialDelay(): Long {
        return 0
    }

    override fun initialize(dependencies: EdmSyncInitializerDependencies) {
        if (dependencies.active) {
            logger.info("Start syncing EDM")
            updateEdm(dependencies.edmManager)
            initializeEntityTypeCollectionsAndApps(dependencies.hazelcast)
            logger.info("Finished syncing EDM")
        }
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(
                PostConstructInitializerTaskDependencies.PostConstructInitializerTask::class.java
        )
    }

    override fun getName(): String {
        return Task.EDM_SYNC_INITIALIZER.name
    }

    override fun getDependenciesClass(): Class<out EdmSyncInitializerDependencies> {
        return EdmSyncInitializerDependencies::class.java
    }

    /**
     * Import EDM from production environment.
     */
    private fun updateEdm(edmManager: EdmManager) {
        val prodRetrofit = RetrofitFactory.newClient(RetrofitFactory.Environment.PRODUCTION)
        val prodEdmApi = prodRetrofit.create(EdmApi::class.java)
        // get prod edm model and remove audit types
        val prodEdm = prodEdmApi.entityDataModel
        if (prodEdm == null) {
            logger.error("Received null EntityDataModel from prod. Either prod is down or EntityDataModel changed.")
            return
        }

        val cleanedProdEdm = removeAuditType(prodEdm)
        val edm = EntityDataModel(
                cleanedProdEdm.namespaces,
                cleanedProdEdm.schemas,
                cleanedProdEdm.entityTypes,
                cleanedProdEdm.associationTypes,
                cleanedProdEdm.propertyTypes)

        // get differences between prod and local
        val edmDiff = edmManager.getEntityDataModelDiff(edm)

        // update with differences
        edmManager.entityDataModel = edmDiff.diff
    }

    private fun initializeEntityTypeCollectionsAndApps(hazelcast: HazelcastInstance) {
        val prodRetrofit = RetrofitFactory.newClient(RetrofitFactory.Environment.PRODUCTION)

        val namesMap = HazelcastMap.NAMES.getMap(hazelcast)
        val aclKeysMap = HazelcastMap.ACL_KEYS.getMap(hazelcast)
        val securableObjectTypesMap = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap(hazelcast)
        val entityTypeCollectionsMap = HazelcastMap.ENTITY_TYPE_COLLECTIONS.getMap(hazelcast)
        val appsMap = HazelcastMap.APPS.getMap(hazelcast)

        /* Initialize EntityTypeCollections */

        val prodCollectionsApi = prodRetrofit.create(CollectionsApi::class.java)
        val entityTypeCollections = prodCollectionsApi.getAllEntityTypeCollections()

        namesMap.putAll(entityTypeCollections.associate { it.id to it.type.fullQualifiedNameAsString })
        aclKeysMap.putAll(entityTypeCollections.associate { it.type.fullQualifiedNameAsString to it.id })
        securableObjectTypesMap.putAll(entityTypeCollections.associate { AclKey(it.id) to SecurableObjectType.EntityTypeCollection })
        entityTypeCollectionsMap.putAll(entityTypeCollections.associateBy { it.id })

        /* Initialize apps */

        val appApi = prodRetrofit.create(AppApi::class.java)
        val apps = appApi.apps

        namesMap.putAll(apps.associate { it.id to it.name })
        aclKeysMap.putAll(apps.associate { it.name to it.id })
        securableObjectTypesMap.putAll(apps.associate { AclKey(it.id) to SecurableObjectType.App })
        appsMap.putAll(apps.associateBy { it.id })
    }

    /**
     * Filter out audit entity sets
     */
    @SuppressFBWarnings(value = ["BC_BAD_CAST_TO_ABSTRACT_COLLECTION"], justification = "Weird bug")
    fun removeAuditType(edm: EntityDataModel): EntityDataModel {
        val propertyTypes = edm.propertyTypes.filter { it.type.toString() != OL_AUDIT_FQN.toString() }
        val entityTypes = edm.entityTypes.filter { it.type.toString() != OL_AUDIT_FQN.toString() }
        val associationTypes = edm.associationTypes.filter {
            it.associationEntityType.type.toString() != OL_AUDIT_FQN.toString()
        }

        return EntityDataModel(
                edm.namespaces,
                edm.schemas,
                entityTypes,
                associationTypes,
                propertyTypes)
    }

}