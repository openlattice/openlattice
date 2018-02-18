/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.edm;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

public class EntityDataModelDiff {

    private final EntityDataModel           diff;
    private final Optional<EntityDataModel> conflicts;

    @JsonCreator
    public EntityDataModelDiff(
            @JsonProperty( SerializationConstants.DIFF ) EntityDataModel diff,
            @JsonProperty( SerializationConstants.CONFLICTS ) Optional<EntityDataModel> conflicts ) {
        this.diff = diff;
        this.conflicts = conflicts;
    }

    @JsonProperty( SerializationConstants.DIFF )
    public EntityDataModel getDiff() {
        return diff;
    }

    @JsonProperty( SerializationConstants.CONFLICTS )
    public Optional<EntityDataModel> getConflicts() {
        return conflicts;
    }

}
