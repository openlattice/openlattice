package com.openlattice.data.storage

import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
fun <T, V> getByDataSource(
        dataSourceResolver: DataSourceResolver,
        entityIds: Map<T, V>,
        entitySetIdReader: (T) -> UUID
): Map<String, Map<T, V>> {
    return entityIds
            .asSequence()
            .groupBy { dataSourceResolver.getDataSourceName(entitySetIdReader(it.key)) }
            .mapValues { dataSourceEntityIds -> dataSourceEntityIds.value.associate { it.toPair() } }
}

fun <T> getByDataSource(
        dataSourceResolver: DataSourceResolver,
        entityIds: Set<T>,
        entitySetIdReader: (T) -> UUID
): Map<String, List<T>> {
    return entityIds.groupBy { dataSourceResolver.getDataSourceName(entitySetIdReader(it)) }
}