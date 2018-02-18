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

package com.openlattice.exceptions;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import java.util.ArrayList;
import java.util.List;

public class ErrorsDTO {
    private List<ErrorDTO> errors;

    public ErrorsDTO() {
        this.errors = new ArrayList<ErrorDTO>();
    }

    public ErrorsDTO( ApiExceptions error, String message ) {
        this();
        this.addError( error, message );
    }

    public void addError( ApiExceptions error, String message ) {
        errors.add( new ErrorDTO( error, message ) );
    }

    @JsonProperty( SerializationConstants.ERRORS )
    public List<ErrorDTO> getErrors() {
        return errors;
    }

    public void setErrors( List<ErrorDTO> errors ) {
        this.errors = errors;
    }

    @Override
    public String toString() {
        return "ErrorsDTO [errors=" + errors + "]";
    }

    @JsonIgnore
    public boolean isEmpty() {
        return errors.isEmpty();
    }
}
