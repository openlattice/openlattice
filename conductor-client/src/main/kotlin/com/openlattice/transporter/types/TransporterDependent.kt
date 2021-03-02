package com.openlattice.transporter.types

interface TransporterDependent<T> {
    companion object {
        const val NOT_INITIALIZED = "Transporter Datastore not initialized."
    }

    fun init(data: TransporterDatastore): T
}