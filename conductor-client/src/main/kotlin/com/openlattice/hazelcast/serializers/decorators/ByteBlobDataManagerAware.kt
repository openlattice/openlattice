package com.openlattice.hazelcast.serializers.decorators

import com.openlattice.data.storage.ByteBlobDataManager

interface ByteBlobDataManagerAware {
    fun setByteBlobDataManager(byteBlobDataManager: ByteBlobDataManager)
}