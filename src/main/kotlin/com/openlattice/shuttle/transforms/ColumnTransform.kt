package com.openlattice.shuttle.transforms

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.shuttle.transformations.Transformation
import java.util.*

class ColumnTransform
/**
 * Represents a transformation to select a column in the original data (i.e. no transform)
 *
 * @param column: column name to collect
 * NOTE: This class is duplicated at transforms.ColumnTransform
 * in shuttle and should be kept in sync
 */
@JsonCreator
constructor(@JsonProperty(SerializationConstants.COLUMN) column: String) : Transformation<Map<String, String>>(Optional.of(column))