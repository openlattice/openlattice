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

package com.openlattice.data

import com.openlattice.data.storage.*
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.PostgresTable
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.streams.toList


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresEntityDataQueryServiceTest {
    private val logger: Logger = LoggerFactory.getLogger(PostgresEntityDataQueryServiceTest::class.java)

    @Test
    fun createEdgesTableQuery() {
        logger.info(PostgresTable.QUERIES.createTableQuery())
        logger.info(PostgresTable.E.createTableQuery())
        logger.info(PostgresTable.SYNC_IDS.createTableQuery())

        logger.info(PostgresTable.E.createIndexQueries.toList().joinToString("\n") { "$it;" })
        logger.info(PostgresTable.SYNC_IDS.createIndexQueries.toList().joinToString("\n") { "$it;" })
        logger.info(PostgresTable.QUERIES.createIndexQueries.toList().joinToString("\n") { "$it;" })
    }


    @Test
    fun testEntitySetLastWriteQuery() {
        val propertyTypes = listOf(
                PropertyType(UUID.fromString("c270d705-3616-4abc-b16e-f891e264b784"), FullQualifiedName("im.PersonNickName"), "PersonNickName", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.String),
                PropertyType(UUID.fromString("7b038634-a0b4-4ce1-a04f-85d1775937aa"), FullQualifiedName("nc.PersonSurName"), "PersonSurName", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.String),
                PropertyType(UUID.fromString("8293b7f3-d89d-44f5-bec2-6397a4c5af8b"), FullQualifiedName("nc.PersonHairColorText"), "PersonHairColorText", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.String),
                PropertyType(UUID.fromString("5260cfbd-bfa4-40c1-ade5-cd83cc9f99b2"), FullQualifiedName("nc.SubjectIdentification"), "SubjectIdentification", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.String),
                PropertyType(UUID.fromString("e9a0b4dc-5298-47c1-8837-20af172379a5"), FullQualifiedName("nc.PersonGivenName"), "PersonGivenName", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.String),
                PropertyType(UUID.fromString("d0935a7e-efd3-4903-b673-0869ef527dea"), FullQualifiedName("nc.PersonMiddleName"), "PersonMiddleName", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.String),
                PropertyType(UUID.fromString("45aa6695-a7e7-46b6-96bd-782e6aa9ac13"), FullQualifiedName("publicsafety.mugshot"), "mugshot", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.Binary),
                PropertyType(UUID.fromString("1e6ff0f0-0545-4368-b878-677823459e57"), FullQualifiedName("nc.PersonBirthDate"), "PersonBirthDate", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.Date),
                PropertyType(UUID.fromString("ac37e344-62da-4b50-b608-0618a923a92d"), FullQualifiedName("nc.PersonEyeColorText"), "PersonEyeColorText", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.String),
                PropertyType(UUID.fromString("481f59e4-e146-4963-a837-4f4e514df8b7"), FullQualifiedName("nc.SSN"), "SSN", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.String),
                PropertyType(UUID.fromString("d9a90e01-9670-46e8-b142-6d0c4871f633"), FullQualifiedName("j.SentenceRegisterSexOffenderIndicator"), "SentenceRegisterSexOffenderIndicator", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.Int32),
                PropertyType(UUID.fromString("d3f3f3de-dc1b-40da-9076-683ddbfeb4d8"), FullQualifiedName("nc.PersonSuffix"), "PersonSuffix", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.String),
                PropertyType(UUID.fromString("f0a6a588-aee7-49a2-8f8e-e5209731da30"), FullQualifiedName("nc.PersonHeightMeasure"), "PersonHeightMeasure", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.Int32),
                PropertyType(UUID.fromString("fa00bfdb-98ec-487a-b62f-f6614e4c921b"), FullQualifiedName("criminaljustice.persontype"), "persontype", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.String),
                PropertyType(UUID.fromString("5ea6e8d5-93bb-47cf-b054-9faaeb05fb27"), FullQualifiedName("person.stateidstate"), "stateidstate", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.String),
                PropertyType(UUID.fromString("6ec154f8-a4a1-4df2-8c57-d98cbac1478e"), FullQualifiedName("nc.PersonSex"), "PersonSex", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.String),
                PropertyType(UUID.fromString("cf4598e7-5bbe-49f7-8935-4b1a692f6111"), FullQualifiedName("nc.PersonBirthPlace"), "PersonBirthPlace", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.String),
                PropertyType(UUID.fromString("32eba813-7d20-4be1-bc1a-717f99917a5e"), FullQualifiedName("housing.notes"), "notes", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.String),
                PropertyType(UUID.fromString("c7d2c503-651d-483f-8c17-72358bcfc5cc"), FullQualifiedName("justice.xref"), "xref", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.String),
                PropertyType(UUID.fromString("f950d05a-f4f2-451b-8c6d-56e78bba8b42"), FullQualifiedName("nc.PersonRace"), "PersonRace", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.String),
                PropertyType(UUID.fromString("314d2bfd-e50e-4965-b2eb-422742fa265c"), FullQualifiedName("housing.updatedat"), "updatedat", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.DateTimeOffset),
                PropertyType(UUID.fromString("1407ac70-ea63-4879-aca4-6722034f0cda"), FullQualifiedName("nc.PersonEthnicity"), "PersonEthnicity", Optional.empty<String>(), setOf(), EdmPrimitiveTypeKind.String)
        )

        logger.info(
                "buildPreparableFiltersSqlForEntities query:\n{}",
                buildPreparableFiltersSql(
                        0,
                        propertyTypes.associateBy { it.id },
                        mapOf(),
                        EnumSet.of(MetadataOption.ENTITY_KEY_IDS),
                        linking = false,
                        idsPresent = true,
                        partitionsPresent = true
                ).first
        )
    }
}
