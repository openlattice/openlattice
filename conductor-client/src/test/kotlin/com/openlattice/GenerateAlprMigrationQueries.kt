package com.openlattice

import org.junit.Test

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class GenerateAlprMigrationQueries {
    private val partitions = 1 until 257
    private val largePartitions = setOf(7, 14, 4, 120, 250, 54, 61, 247, 93, 228, 215, 182, 212, 209, 130, 214, 141)

    private val alreadyProcessed = (1 until 11).toSet()

    @Test
    fun generateSql() {
        for (partition in partitions) {
            val limits = if (partition in largePartitions) {
                listOf(0, 10, 20, 30)
            } else {
                listOf(0, 10, 20)
            }.toMutableList()
            if (partition in alreadyProcessed) {
                limits.remove(0)
            }
            limits.map { limit ->
                """
                    DROP VIEW alprdata_P${partition}_${limit}M;
                """.trimIndent()
//                """
//   DROP FOREIGN TABLE citusprod.alprdata_P${partition}_${limit}M;
//                """.trimIndent()
//                """
//            CREATE OR REPLACE VIEW alprdata_P${partition}_${limit}M as
//            SELECT * from data
//            WHERE PARTITION = $partition AND entity_set_id='2d20ecf8-17ca-43b5-9d49-1721b2e79cc7' OFFSET ${limit * 1000000} LIMIT 10000000;
//                """.trimIndent()
//                """
//   GRANT ALL ON alprdata_P${partition}_${limit}M to openlattice;
//                """.trimIndent()
//                            """
//   import foreign schema public LIMIT TO (alprdata_P${partition}_${limit}M) FROM SERVER citusprod into citusprod;
//                """.trimIndent()
//                """
//                    INSERT INTO public.data SELECT entity_set_id, id, origin_id, partition, property_type_id, hash, last_write, last_propagate, last_transport, VERSION, versions, b_text, b_uuid, b_smallint, b_integer, b_bigint, b_date, b_timestamptz, b_double, b_boolean, b_timetz, n_text, n_uuid, n_smallint, n_integer, n_bigint, n_date, n_timestamptz, n_double, n_boolean, n_timetz FROM citusprod.alprdata_P${partition}_${limit}M;
//                """.trimIndent()
            }.forEach {
                println(it)
            }
        }
    }

    @Test
    fun generateEdgeMigrationSql() {
        for (partition in partitions) {
            val limits = listOf(0, 10)
            limits.map { limit ->
//                """
//CREATE OR REPLACE VIEW alpredges_P${partition}_${limit}M as
//SELECT * from E
//WHERE PARTITION = $partition
//AND (edge_entity_set_id = '699dcac4-1e75-47ff-a127-c3d124031e13'
//OR (src_entity_set_id='2d20ecf8-17ca-43b5-9d49-1721b2e79cc7' OR dst_entity_set_id='2d20ecf8-17ca-43b5-9d49-1721b2e79cc7')
//OR (src_entity_set_id='04693624-c17b-4393-86f3-39cb0038e830' OR dst_entity_set_id='04693624-c17b-4393-86f3-39cb0038e830'))
//OFFSET ${limit * 1000000} LIMIT 10000000;
//                """.trimIndent() .replace("\n"," ")
//                """
//   DROP FOREIGN TABLE citusprod.alprdata_P${partition}_${limit}M;
//                """.trimIndent()
                """
    DROP VIEW alpredges_P${partition}_${limit}M;
""".trimIndent()
//                """
//            CREATE OR REPLACE VIEW alprdata_P${partition}_${limit}M as
//            SELECT * from data
//            WHERE PARTITION = $partition AND entity_set_id='2d20ecf8-17ca-43b5-9d49-1721b2e79cc7' OFFSET ${limit * 1000000} LIMIT 10000000;
//                """.trimIndent()
//                """
//   GRANT ALL ON alpredges_P${partition}_${limit}M to openlattice;
//                """.trimIndent()
//                            """
//   import foreign schema public LIMIT TO (alpredges_P${partition}_${limit}M) FROM SERVER citusprod into citusprod;
//                """.trimIndent()
//                """
//                    INSERT INTO public.E SELECT * FROM citusprod.alpredges_P${partition}_${limit}M;
//                """.trimIndent()
            }.forEach {
                println(it)
            }
        }
    }

    @Test
    fun generateHasMigrationSql() {
        for (partition in partitions) {
            val limits = listOf(0)
            limits.map { limit ->
//                """
//CREATE OR REPLACE VIEW alpredges_P${partition}_${limit}M as
//SELECT * from E
//WHERE PARTITION = $partition
//AND (edge_entity_set_id = '699dcac4-1e75-47ff-a127-c3d124031e13'
//OR (src_entity_set_id='2d20ecf8-17ca-43b5-9d49-1721b2e79cc7' OR dst_entity_set_id='2d20ecf8-17ca-43b5-9d49-1721b2e79cc7')
//OR (src_entity_set_id='04693624-c17b-4393-86f3-39cb0038e830' OR dst_entity_set_id='04693624-c17b-4393-86f3-39cb0038e830'))
//OFFSET ${limit * 1000000} LIMIT 10000000;
//                """.trimIndent() .replace("\n"," ")
//                """
//   DROP FOREIGN TABLE citusprod.alprdata_P${partition}_${limit}M;
//                """.trimIndent()
//                """
//            CREATE OR REPLACE VIEW alprhas_P${partition}_${limit}M as
//            SELECT * from data
//            WHERE PARTITION = $partition AND entity_set_id='699dcac4-1e75-47ff-a127-c3d124031e13' OFFSET ${limit * 1000000} LIMIT 10000000;
//                """.replace("\n","").trimIndent() +
//                """
//   GRANT ALL ON alprhas_P${partition}_${limit}M to openlattice;
//                """.trimIndent()
//                            """
//   import foreign schema public LIMIT TO (alprhas_P${partition}_${limit}M) FROM SERVER citusprod into citusprod;
//                """.trimIndent()
                """
                    INSERT INTO public.data SELECT entity_set_id, id, origin_id, partition, property_type_id, hash, last_write, last_propagate, last_transport, VERSION, versions, b_text, b_uuid, b_smallint, b_integer, b_bigint, b_date, b_timestamptz, b_double, b_boolean, b_timetz, n_text, n_uuid, n_smallint, n_integer, n_bigint, n_date, n_timestamptz, n_double, n_boolean, n_timetz FROM citusprod.alprhas_P${partition}_${limit}M;
                """.trimIndent()
            }.forEach {
                println(it)
            }
        }
    }

    @Test
    fun generateDataDeletionMigrationSql() {
//        partitions
            listOf(257)
                .map { partition ->
            val filterSql = """
                partition = $partition
                AND entity_set_id = ANY('{2d20ecf8-17ca-43b5-9d49-1721b2e79cc7,699dcac4-1e75-47ff-a127-c3d124031e13,04693624-c17b-4393-86f3-39cb0038e830}')
            """.replace("\n","").trimIndent()
            """
                CREATE TABLE IF NOT EXISTS for_deletion_$partition AS SELECT * FROM DATA WHERE $filterSql;
                GRANT ALL ON for_deletion_$partition TO OPENLATTICE;
                DELETE FROM DATA WHERE $filterSql;
            """.replace("\n","").trimIndent()
//              """
//   import foreign schema public LIMIT TO (for_deletion_$partition) FROM SERVER citusprod into citusprod;
//                """.trimIndent()
//            """
//                BEGIN; INSERT INTO data SELECT entity_set_id, id, origin_id, partition, property_type_id, hash, last_write, last_propagate, last_transport, VERSION, versions, b_text, b_uuid, b_smallint, b_integer, b_bigint, b_date, b_timestamptz, b_double, b_boolean, b_timetz, n_text, n_uuid, n_smallint, n_integer, n_bigint, n_date, n_timestamptz, n_double, n_boolean, n_timetz FROM citusprod.for_deletion_$partition WHERE (select not migrated from migration_progress where partition = $partition) ON CONFLICT DO NOTHING;
//                UPDATE migration_progress SET migrated = true WHERE partition = $partition; end transaction;
//            """.trimIndent()
        }.forEach {
            println(it)
        }
    }


    @Test
    fun generateEdgeDeletionMigrationSql() {
        partitions.map { partition ->
            val filterSql = """
            partition = $partition
            AND (edge_entity_set_id = '699dcac4-1e75-47ff-a127-c3d124031e13' 
            OR (src_entity_set_id='2d20ecf8-17ca-43b5-9d49-1721b2e79cc7' OR dst_entity_set_id='2d20ecf8-17ca-43b5-9d49-1721b2e79cc7')
            OR (src_entity_set_id='04693624-c17b-4393-86f3-39cb0038e830' OR dst_entity_set_id='04693624-c17b-4393-86f3-39cb0038e830'))
        """.trimIndent()
            """
                CREATE TABLE IF NOT EXIST edges_for_deletion_$partition AS SELECT * FROM WHERE $filterSql;
                GRANT ALL ON edges_for_deletion_$partition TO openlattice;
                DELETE FROM e WHERE $filterSql;
            """.trimIndent()
        }.forEach {
            println(it)
        }
    }
}