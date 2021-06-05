package com.openlattice.shuttle

open class BaseBuilder<ParentBuilder, BuiltObject>(
        private val parentBuilder: ParentBuilder,
        private val builderCallback: BuilderCallback<BuiltObject>
) {

    protected fun ok(`object`: BuiltObject): ParentBuilder {
        this.builderCallback.onBuild(`object`)
        return this.parentBuilder
    }
}