package com.openlattice.shuttle.util

import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvSchema

class CsvUtil {
    companion object {
        @JvmStatic
        fun newDefaultMapper(): CsvMapper {
            return CsvMapper()
        }

        @JvmStatic
        fun newDefaultSchemaFromHeader(): CsvSchema {
            return CsvSchema.emptySchema().withHeader()
        }
    }
}