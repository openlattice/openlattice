

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
 */

package com.openlattice.datastore.exceptions;

import java.util.List;

import org.springframework.http.HttpStatus;

import com.openlattice.exceptions.ErrorDTO;
import com.openlattice.exceptions.ErrorsDTO;
import com.openlattice.exceptions.ApiExceptions;

public class BatchException extends RuntimeException {
    private static final long serialVersionUID = 7632884063119454460L;
    private ErrorsDTO         errors           = new ErrorsDTO();
    private HttpStatus        statusCode;

    public BatchException( ErrorsDTO errors ) {
        this( errors, HttpStatus.INTERNAL_SERVER_ERROR );
    }

    public BatchException( List<ErrorDTO> list ) {
        this( list, HttpStatus.INTERNAL_SERVER_ERROR );
    }

    public BatchException( ErrorsDTO errors, HttpStatus statusCode ) {
        this.errors = errors;
        this.statusCode = statusCode;
    }

    public BatchException( List<ErrorDTO> list, HttpStatus statusCode ) {
        this.errors.setErrors( list );
        this.statusCode = statusCode;
    }

    public ErrorsDTO getErrors() {
        return errors;
    }

    public void addError( ApiExceptions error, String message ) {
        errors.addError( error, message );
    }

    public void setErrors( ErrorsDTO errors ) {
        this.errors = errors;
    }

    public void setErrors( List<ErrorDTO> list ) {
        this.errors.setErrors( list );
    }

    public HttpStatus getStatusCode() {
        return statusCode;
    }

}
