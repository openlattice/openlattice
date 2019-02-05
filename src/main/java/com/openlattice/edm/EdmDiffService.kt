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

package com.openlattice.edm

import com.openlattice.datastore.services.EdmManager

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class EdmDiffService(private val edm: EdmManager) {
    fun diff(otherDataModel: EntityDataModel): EdmDiff {
        val currentDataModel = edm.entityDataModel!!
        return matchingVersionDiff(currentDataModel, otherDataModel)
    }

    private fun differentVersionDiff(currentDataModel: EntityDataModel, otherDataModel: EntityDataModel): EdmDiff {
        //Since the versions are different we will do our best using FQNs.
        val currentPropertyTypes = currentDataModel.propertyTypes.asSequence().map { it.type to it }.toMap()
        val currentEntityTypes = currentDataModel.entityTypes.asSequence().map { it.type to it }.toMap()
        val currentAssociationTypes = currentDataModel.associationTypes.asSequence().map { it.associationEntityType.type to it }.toMap()
        val currentSchemas = currentDataModel.schemas.map { it.fqn to it }.toMap()
        val currentNamespaces = currentDataModel.namespaces.toSet()

        val presentPropertyTypes = otherDataModel.propertyTypes.filter { currentPropertyTypes.keys.contains(it.type) }
        val presentEntityTypes = otherDataModel.entityTypes.filter { currentEntityTypes.keys.contains(it.type) }
        val presentAssociationTypes = otherDataModel.associationTypes.filter {
            currentAssociationTypes.keys.contains(
                    it.associationEntityType.type
            )
        }
        val presentSchemas = otherDataModel.schemas.asIterable().filter { currentSchemas.contains(it.fqn) }
        val presentNamespaces = otherDataModel.namespaces.minus(currentNamespaces)

        val missingPropertyTypes = currentDataModel.propertyTypes.filter { !currentPropertyTypes.contains(it.type) }
        val missingEntityTypes = currentDataModel.entityTypes.filter { !currentEntityTypes.contains(it.type) }
        val missingAssociationTypes = currentDataModel.associationTypes.filter {
            !currentAssociationTypes.contains(
                    it.associationEntityType.type
            )
        }
        val missingSchemas = currentDataModel.schemas.filter { !currentSchemas.contains(it.fqn) }
        val missingNamespaces = currentDataModel.namespaces.minus(currentNamespaces)

        val conflictingPropertyTypes = otherDataModel.propertyTypes
                .asSequence()
                .map { it to currentPropertyTypes[it.type] }
                .filter { it.first == it.second }
                .map { it.first }
                .toSet()
        val conflictingEntityTypes = otherDataModel.entityTypes
                .asSequence()
                .mapNotNull {
                    it to currentEntityTypes[it.type]
                }
                .filter { it.second != null && it.first == it.second }
                .map { it.first }
                .toSet()
        val conflictingAssociationTypes = otherDataModel.associationTypes
                .asSequence()
                .map { it to currentAssociationTypes[it.associationEntityType.type] }
                .filter { it.second != null && it.first == it.second }
                .map { it.first }
                .toSet()
        val conflictingSchemas = otherDataModel.schemas.filter {
            currentSchemas.containsKey(it.fqn)
                    && it.propertyTypes == currentSchemas[it.fqn]?.propertyTypes
                    && it.entityTypes == currentSchemas[it.fqn]?.entityTypes
        }

        //Namespaces cannot conflict.
        return EdmDiff(
                EntityDataModel(
                        presentNamespaces,
                        presentSchemas,
                        presentEntityTypes,
                        presentAssociationTypes,
                        presentPropertyTypes
                ),
                EntityDataModel(
                        missingNamespaces,
                        missingSchemas,
                        missingEntityTypes,
                        missingAssociationTypes,
                        missingPropertyTypes
                ),
                EntityDataModel(
                        listOf(),
                        conflictingSchemas,
                        conflictingEntityTypes,
                        conflictingAssociationTypes,
                        conflictingPropertyTypes
                )
        )
    }

    private fun matchingVersionDiff(currentDataModel: EntityDataModel, otherDataModel: EntityDataModel): EdmDiff {
        //Since the versions are same we use ids
        val currentPropertyTypes = currentDataModel.propertyTypes.asSequence().map { it.id to it }.toMap()
        val currentEntityTypes = currentDataModel.entityTypes.asSequence().map { it.id to it }.toMap()
        val currentAssociationTypes = currentDataModel.associationTypes.asSequence().map { it.associationEntityType.id to it }.toMap()
        val currentSchemas = currentDataModel.schemas.map { it.fqn to it }.toMap()
        val currentNamespaces = currentDataModel.namespaces.toSet()

        val presentPropertyTypes = otherDataModel.propertyTypes.filter { currentPropertyTypes.keys.contains(it.id) }
        val presentEntityTypes = otherDataModel.entityTypes.filter { currentEntityTypes.keys.contains(it.id) }
        val presentAssociationTypes = otherDataModel.associationTypes.filter {
            currentAssociationTypes.keys.contains(
                    it.associationEntityType.id
            )
        }
        val presentSchemas = otherDataModel.schemas.asIterable().filter { currentSchemas.contains(it.fqn) }
        val presentNamespaces = otherDataModel.namespaces.minus(currentNamespaces)

        val missingPropertyTypes = currentDataModel.propertyTypes.filter { !currentPropertyTypes.contains(it.id) }
        val missingEntityTypes = currentDataModel.entityTypes.filter { !currentEntityTypes.contains(it.id) }
        val missingAssociationTypes = currentDataModel.associationTypes.filter {
            !currentAssociationTypes.contains(
                    it.associationEntityType.id
            )
        }
        val missingSchemas = currentDataModel.schemas.filter { !currentSchemas.contains(it.fqn) }
        val missingNamespaces = currentDataModel.namespaces.minus(currentNamespaces)

        val conflictingPropertyTypes = otherDataModel.propertyTypes
                .asSequence()
                .map { it to currentPropertyTypes[it.id] }
                .filter { it.first == it.second }
                .map { it.first }
                .toSet()
        val conflictingEntityTypes = otherDataModel.entityTypes
                .asSequence()
                .mapNotNull {
                    it to currentEntityTypes[it.id]
                }
                .filter { it.second != null && it.first == it.second }
                .map { it.first }
                .toSet()
        val conflictingAssociationTypes = otherDataModel.associationTypes
                .asSequence()
                .map { it to currentAssociationTypes[it.associationEntityType.id] }
                .filter { it.second != null && it.first == it.second }
                .map { it.first }
                .toSet()

        val conflictingSchemas = otherDataModel.schemas.filter {
            currentSchemas.containsKey(it.fqn)
                    && it.propertyTypes == currentSchemas[it.fqn]?.propertyTypes
                    && it.entityTypes == currentSchemas[it.fqn]?.entityTypes
        }

        //Namespaces cannot conflict.
        return EdmDiff(
                EntityDataModel(
                        presentNamespaces,
                        presentSchemas,
                        presentEntityTypes,
                        presentAssociationTypes,
                        presentPropertyTypes
                ),
                EntityDataModel(
                        missingNamespaces,
                        missingSchemas,
                        missingEntityTypes,
                        missingAssociationTypes,
                        missingPropertyTypes
                ),
                EntityDataModel(
                        listOf(),
                        conflictingSchemas,
                        conflictingEntityTypes,
                        conflictingAssociationTypes,
                        conflictingPropertyTypes
                )
        )
    }
}