package com.openlattice.transporter.types

interface TransporterDependent {
    companion object {
        const val NOT_INITIALIZED = "Transporter Datastore not initialized."
    }

    fun init(data: TransporterDatastore)
}