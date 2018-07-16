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

package com.openlattice.datastore.util;

import com.openlattice.authorization.ForbiddenException;
import com.openlattice.datastore.exceptions.BatchException;
import com.openlattice.datastore.exceptions.ResourceNotFoundException;
import com.openlattice.edm.exceptions.TypeExistsException;
import com.openlattice.exceptions.ApiExceptions;
import com.openlattice.exceptions.ErrorsDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class DataStoreExceptionHandler {
    private static final Logger logger    = LoggerFactory.getLogger( DataStoreExceptionHandler.class );
    private static final String ERROR_MSG = "";

    @ExceptionHandler( { NullPointerException.class, ResourceNotFoundException.class } )
    public ResponseEntity<ErrorsDTO> handleNotFoundException( Exception e ) {
        logger.error( ERROR_MSG, e );
        if ( e.getMessage() != null ) {
            return new ResponseEntity<>(
                    new ErrorsDTO( ApiExceptions.RESOURCE_NOT_FOUND_EXCEPTION, e.getMessage() ),
                    HttpStatus.NOT_FOUND );
        }
        return new ResponseEntity<>( HttpStatus.NOT_FOUND );
    }

    @ExceptionHandler( { IllegalArgumentException.class, HttpMessageNotReadableException.class } )
    public ResponseEntity<ErrorsDTO> handleIllegalArgumentException( Exception e ) {
        logger.error( ERROR_MSG, e );
        return new ResponseEntity<>(
                new ErrorsDTO( ApiExceptions.ILLEGAL_ARGUMENT_EXCEPTION, e.getMessage() ),
                HttpStatus.BAD_REQUEST );
    }

    @ExceptionHandler( IllegalStateException.class )
    public ResponseEntity<ErrorsDTO> handleIllegalStateException( Exception e ) {
        logger.error( ERROR_MSG, e );
        return new ResponseEntity<>(
                new ErrorsDTO( ApiExceptions.ILLEGAL_STATE_EXCEPTION, e.getMessage() ),
                HttpStatus.INTERNAL_SERVER_ERROR );
    }

    @ExceptionHandler( TypeExistsException.class )
    public ResponseEntity<ErrorsDTO> handleTypeExistsException( Exception e ) {
        logger.error( ERROR_MSG, e );
        return new ResponseEntity<>(
                new ErrorsDTO( ApiExceptions.TYPE_EXISTS_EXCEPTION, e.getMessage() ),
                HttpStatus.CONFLICT );
    }

    @ExceptionHandler( ForbiddenException.class )
    public ResponseEntity<ErrorsDTO> handleUnauthorizedExceptions( ForbiddenException e ) {
        logger.error( ERROR_MSG, e );
        return new ResponseEntity<>(
                new ErrorsDTO( ApiExceptions.FORBIDDEN_EXCEPTION, e.getMessage() ),
                HttpStatus.UNAUTHORIZED );
    }

    @ExceptionHandler( BatchException.class )
    public ResponseEntity<ErrorsDTO> handleBatchExceptions( BatchException e ) {
        logger.error( ERROR_MSG, e );
        return new ResponseEntity<ErrorsDTO>( e.getErrors(), e.getStatusCode() );
    }

    @ExceptionHandler( Exception.class )
    public ResponseEntity<ErrorsDTO> handleOtherExceptions( Exception e ) {
        logger.error( ERROR_MSG, e );
        return new ResponseEntity<>(
                new ErrorsDTO( ApiExceptions.OTHER_EXCEPTION, e.getClass().getSimpleName() + ": " + e.getMessage() ),
                HttpStatus.INTERNAL_SERVER_ERROR );
    }
}
