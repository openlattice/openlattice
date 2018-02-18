

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

package com.openlattice.conductor.rpc;

import java.io.IOException;
import java.io.Serializable;
import java.text.NumberFormat;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

@JsonPropertyOrder( { "name", "title", "dept", "salary" } )
public class Employee implements Serializable {
    private static final long serialVersionUID = 8066356339798185695L;

    public static class EmployeeCsvReader {
        private static final transient CsvMapper mapper = new CsvMapper();
        private static final transient CsvSchema schema = mapper.schemaFor( Employee.class );
        private static final Logger              logger = LoggerFactory.getLogger( EmployeeCsvReader.class );

        static {
            logger.info( "Schema: {}", schema.getColumnDesc() );
        }

        public static Employee getEmployee( String row ) throws IOException {
            try {
                return mapper.reader( Employee.class ).with( schema ).readValue( row );
            } catch ( IOException e ) {
                logger.error( "Something went wrong parsing row: {}", row, e );
                throw e;
            }
        }
    }

    private static final NumberFormat CONVERTER = NumberFormat.getCurrencyInstance();
    private final String              name;
    private final String              title;
    private final String              dept;
    private final int                 salary;

    @JsonCreator
    public Employee(
            @JsonProperty( "name" ) String name,
            @JsonProperty( "title" ) String title,
            @JsonProperty( "dept" ) String dept,
            @JsonProperty( "salary" ) String salary ) {
        this.name = name;
        this.title = title;
        this.dept = dept;
        int s;
        try {
            s = CONVERTER.parse( salary ).intValue();
        } catch ( ArrayIndexOutOfBoundsException | ParseException | NumberFormatException e ) {
            s = 0;
        }
        this.salary = s;
    }

    public Employee(
            String name,
            String title,
            String dept,
            int salary ) {
        this.name = name;
        this.title = title;
        this.dept = dept;
        this.salary = salary;
    }

    @JsonProperty( "name" )
    public String getName() {
        return name;
    }

    @JsonProperty( "title" )
    public String getTitle() {
        return title;
    }

    @JsonProperty( "dept" )
    public String getDept() {
        return dept;
    }

    @JsonProperty( "salary" )
    public int getSalary() {
        return salary;
    }

    @Override
    public String toString() {
        return "Employee [name=" + name + ", title=" + title + ", dept=" + dept + ", salary=" + salary + "]";
    }

}
