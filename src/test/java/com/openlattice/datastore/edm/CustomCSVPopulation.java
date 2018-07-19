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

package com.openlattice.datastore.edm;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.data.storage.HazelcastEntityDatastore;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.ODataStorageService;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.Schema;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

public class CustomCSVPopulation {
    public static final    String            NAMESPACE = "stressTest";
    public static final Principal             principal       = new Principal(
            PrincipalType.USER,
            "tests|blahblah" );
    protected static final DatastoreServices ds        = new DatastoreServices();
    public static int numPropertyTypes;
    public static int numEntityTypes;
    public static int numEntitySets;
    public static int numRows;
    public static String individualResultLoc = "src/test/resources/allResult.txt";
    public static String averageResultLoc    = "src/test/resources/averageResult.txt";
    public static int                      defaultTypeSize = 0;
    public static List<CustomPropertyType> defaultTypeList = new ArrayList<>();
    public static List<CustomPropertyType> propertyTypesList = new ArrayList<>();
    public static CsvSchema csvSchema;
    public static List<EntityType>    EntityTypesList = new ArrayList<>();
    public static List<String>        EntitySetsList  = new ArrayList<String>();
    public static Map<String, String> EntitySetToType = new HashMap<>();
    // Random
    @SuppressWarnings( "rawtypes" )
    public static       Map<String, Supplier> RandomGenerator = new HashMap<>();
    // Partition Key Count
    public static       int                   partitionKey    = 0;
    static EdmManager               dms;
    static ODataStorageService      odsc;
    static HazelcastEntityDatastore cdm;
    static HazelcastSchemaManager   hsm;

    // @Test
    public void TestGetAllEntitiesOfType() throws IOException {
        // Time getAllEntitiesOfType 10 times
        timeGetAllEntitiesOfType( 10 );
        // Go to src/test/resources/{allResult.text, averageResult.txt} for test results.
    }

    public static void loadDefaultPropertyTypes() {
        defaultTypeList.add( new CustomPropertyType(
                "age",
                EdmPrimitiveTypeKind.Int32,
                "age",
                () -> ( new Random() ).nextInt( 120 ),
                "Integer" ) );

        defaultTypeList.add( new CustomPropertyType(
                "alpha",
                EdmPrimitiveTypeKind.String,
                "alpha",
                () -> RandomStringUtils.randomAlphabetic( 8 ),
                "String" ) );
        /**
         * defaultTypeList.add( new CustomPropertyType("bool", EdmPrimitiveTypeKind.Boolean, "bool", () -> (new
         * Random()).nextBoolean(), "Boolean" ) ); multiplicityOfDefaultType.add(0);
         */
        defaultTypeList.add( new CustomPropertyType(
                "char",
                EdmPrimitiveTypeKind.String,
                "char",
                () -> RandomStringUtils.randomAlphabetic( 1 ),
                "String" ) );
        /**
         * defaultTypeList.add( new CustomPropertyType("digit", EdmPrimitiveTypeKind.Int32, "digit", () -> (new
         * Random()).nextInt(9), "Integer" ) ); multiplicityOfDefaultType.add(0);
         */
        defaultTypeList.add( new CustomPropertyType(
                "float",
                EdmPrimitiveTypeKind.Double,
                "float",
                () -> ( new Random() ).nextFloat(),
                "Double" ) );

        defaultTypeList.add(
                new CustomPropertyType( "guid", EdmPrimitiveTypeKind.Guid, "guid", () -> UUID.randomUUID(), "UUID" ) );

        defaultTypeList.add( new CustomPropertyType(
                "integer",
                EdmPrimitiveTypeKind.Int32,
                "integer",
                () -> ( new Random() ).nextInt( 123456 ),
                "Integer" ) );

        defaultTypeList.add( new CustomPropertyType(
                "string",
                EdmPrimitiveTypeKind.String,
                "string",
                () -> RandomStringUtils.randomAscii( 10 ),
                "String" ) );

        defaultTypeSize = defaultTypeList.size();
    }

    /**
     * @param n Generate n Property Types Every type would be generated from the default Types Multiplicities of default
     * Types generated would be recorded, and append to the name of generated property type
     */
    public static List<CustomPropertyType> generatePropertyTypes( int n ) {
        numPropertyTypes = n;

        Random rand = new Random();
        for ( int i = 0; i < n; i++ ) {
            int index = rand.nextInt( defaultTypeSize );

            CustomPropertyType propertyType = defaultTypeList.get( index );
            String newName = propertyType.getName() + "_" + Math.abs( rand.nextLong() );
            EdmPrimitiveTypeKind dataType = propertyType.getDataType();
            String keyword = propertyType.getKeyword();
            Callable randomGenCallable = propertyType.getRandomGenCallable();
            String javaTypeName = propertyType.getJavaTypeName();

            propertyTypesList
                    .add( new CustomPropertyType(
                            UUID.randomUUID(),
                            newName,
                            dataType,
                            keyword,
                            randomGenCallable,
                            javaTypeName ) );
        }
        return propertyTypesList;
    }

    public static void generateCSV( int n, String location ) throws Exception {
        numRows = n;
        // Build CSV Schema
        CsvSchema.Builder schemaBuilder = CsvSchema.builder();
        for ( CustomPropertyType type : propertyTypesList ) {
            schemaBuilder.addColumn( type.getName() );
        }
        csvSchema = schemaBuilder.build();

        // Write to CSV
        CsvMapper mapper = new CsvMapper();
        ObjectWriter myObjectWriter = mapper.writer( csvSchema );

        File tempFile = new File( location );
        if ( tempFile.exists() ) {
            tempFile.delete();
        }
        tempFile.createNewFile();

        FileOutputStream tempFileOutputStream = new FileOutputStream( tempFile );
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream( tempFileOutputStream, 1024 );
        OutputStreamWriter writerOutputStream = new OutputStreamWriter( bufferedOutputStream, "UTF-8" );

        List<List<Object>> values = new ArrayList<List<Object>>();
        for ( int i = 0; i < numRows; i++ ) {
            List<Object> rowValues = new ArrayList<Object>();
            for ( CustomPropertyType type : propertyTypesList ) {
                rowValues.add( type.getRandom() );
            }
            values.add( rowValues );
        }
        myObjectWriter.writeValue( writerOutputStream, values );
        System.out.println( "CSV generated \n" );
    }

    public static void createPropertyTypes() {
        for ( CustomPropertyType type : propertyTypesList ) {
            dms.createPropertyTypeIfNotExists( new PropertyType(
                    type.getFqn(),
                    "Generated Type " + type.getFqn().toString(),
                    Optional.empty(),
                    ImmutableSet.of(),
                    type.getDataType() ) );
        }
    }

    /**
     * @param n Create n Entity Types, each with all the existing property types.
     * @param m Create m Entity Sets for each Entity Type Default setting: * Entity Type has 10-character names * Each
     * Entity Type has
     */
    public static void createEntityTypes( int n, int m ) {
        numEntityTypes = n;
        numEntitySets = m;
        for ( int i = 0; i < numEntityTypes; i++ ) {
            // Entity Type of 10-character names
            String entityTypeName = RandomStringUtils.randomAlphabetic( 10 );
            LinkedHashSet<UUID> keyPropertyType = Sets.newLinkedHashSet();
            keyPropertyType.add( propertyTypesList.get( propertyTypesList.size() - 1 ).getId() );
            LinkedHashSet<UUID> propertyTypeIdsSet = propertyTypesList.stream().map( pt -> pt.getId() )
                    .collect( Collectors.toCollection( LinkedHashSet::new ) );

            UUID entityTypeId = UUID.randomUUID();
            EntityType entityType = new EntityType(
                    entityTypeId,
                    new FullQualifiedName( NAMESPACE, entityTypeName ),
                    entityTypeName,
                    Optional.empty(),
                    ImmutableSet.of(),
                    keyPropertyType,
                    propertyTypeIdsSet,
                    Optional.empty(),
                    Optional.of( SecurableObjectType.EntityType ) );
            // Add property types to entity type

            // Create Entity Type in database
            dms.createEntityType( entityType );

            // Update list of custom Entity Types
            EntityTypesList.add( entityType );

            // Create entity set
            for ( int j = 0; j < numEntitySets; j++ ) {
                String entitySetName = RandomStringUtils.randomAlphabetic( 10 );
                // Create entity set
                dms.createEntitySet( principal,
                        new EntitySet(
                                entityTypeId,
                                entitySetName,
                                "Random Entity Set " + entitySetName,
                                Optional.empty(),
                                ImmutableSet.of( "foo@bar.com" ) ) );

                // Update list of custom Entity Sets
                EntitySetsList.add( entitySetName );
                // Update entity set to type map
                EntitySetToType.put( entitySetName, entityTypeName );
            }
        }
    }

    public static void createSchema() {
        Set<PropertyType> propertyTypesSet = propertyTypesList.stream()
                .map( type -> new PropertyType(
                        type.getFqn(),
                        "Generated Type " + type.getFqn().toString(),
                        Optional.empty(),
                        ImmutableSet.of(),
                        type.getDataType() ) )
                .collect( Collectors.toSet() );

        hsm.createOrUpdateSchemas(
                new Schema( new FullQualifiedName( NAMESPACE, "hochung" ), propertyTypesSet, EntityTypesList ) );
    }

    private static Object TypeConversion( String str, String type ) {
        // Convert string to the corresponding type, guaranteed that the string can be converted to that type.
        switch ( type ) {
            case "Integer":
                return Integer.parseInt( str );
            case "Boolean":
                return Boolean.parseBoolean( str );
            case "Byte":
                return Byte.parseByte( str );
            case "Double":
                return Double.parseDouble( str );
            case "UUID":
                return UUID.fromString( str );
            case "Long":
                return Long.parseLong( str );
            default:
                return str;
        }
    }

    public static void writeCSVToDB( String location ) throws JsonProcessingException, IOException {
        /**
         * int numOfEntitySets = EntitySetsList.size(); Random rand = new Random();
         *
         * CsvMapper mapper = new CsvMapper(); // important: we need "array wrapping" (see next section) here:
         * mapper.enable( CsvParser.Feature.WRAP_AS_ARRAY ); File csvFile = new File( location ); // or from String, URL
         * etc
         *
         * MappingIterator<Map<String, String>> it = mapper.readerFor( new TypeReference<Map<String, String>>() {} )
         * .with( csvSchema ).readValues( csvFile ); while ( it.hasNext() ) { Entity entity = new Entity(); String
         * entitySetName = EntitySetsList.get( rand.nextInt( numOfEntitySets ) ); String entityTypeName =
         * EntitySetToType.get( entitySetName ); FullQualifiedName entityTypeFQN = new FullQualifiedName( NAMESPACE,
         * entityTypeName );
         *
         * entity.setType( entityTypeFQN.getFullQualifiedNameAsString() ); Map<String, String> map = it.next();
         *
         * for ( CustomPropertyType propertyType : propertyTypesList ) { Property property = new Property(); String
         * propertyName = propertyType.getName();
         *
         * property.setName( propertyName ); property.setType( new FullQualifiedName( NAMESPACE, propertyName
         * ).getFullQualifiedNameAsString() ); property.setValue( ValueType.PRIMITIVE, TypeConversion( map.get(
         * propertyName ), propertyType.getJavaTypeName() ) );
         *
         * entity.addProperty( property ); } odsc.replaceEntities( ACLs.EVERYONE_ACL, Syncs.BASE.getSyncId(),
         * entitySetName, entityTypeFQN, entity ); }
         */
    }

    /**
     * WARNING: THIS TEST IS DISABLED SINCE getAllEntitiesOfType IS NOW GONE. Benchmarking getAllEntitiesOfType
     */
    public static void timeGetAllEntitiesOfType( int numTest ) throws IOException {
        /**
         * // Initialize file writers File fileAll = new File( individualResultLoc ); FileWriter fwAll = new FileWriter(
         * fileAll.getAbsoluteFile() ); BufferedWriter bwAll = new BufferedWriter( fwAll );
         *
         * File fileAverage = new File( averageResultLoc ); FileWriter fwAverage = new FileWriter(
         * fileAverage.getAbsoluteFile() ); BufferedWriter bwAverage = new BufferedWriter( fwAverage );
         *
         * bwAll.write( "========================================================== \n" ); bwAll.write( "Testing:
         * getAllEntitiesOfType \n" ); bwAll.write( "Number of Columns: " + numPropertyTypes + " \n" ); bwAll.write(
         * "Number of Rows: " + numRows + " \n" ); bwAll.write( "Number of Entity Types: " + numEntityTypes + " \n" );
         * bwAll.write( "Number of Entity Sets: " + numEntitySets + " \n" ); bwAll.write(
         * "========================================================== \n" ); bwAll.write( "Test #, Time elapsed (ms)
         * \n" );
         *
         * bwAverage.write( "========================================================== \n" ); bwAverage.write(
         * "Testing: getAllEntitiesOfType \n" ); bwAverage.write( "Number of Columns: " + numPropertyTypes + " \n" );
         * bwAverage.write( "Number of Rows: " + numRows + " \n" ); bwAverage.write( "Number of Entity Types: " +
         * numEntityTypes + " \n" ); bwAverage.write( "Number of Entity Sets: " + numEntitySets + " \n" );
         * bwAverage.write( "========================================================== \n" );
         *
         * // Actual testing float totalTime = 0;
         *
         * for ( int i = 0; i < numTest; i++ ) { // Decide which EntityType to look up String entityTypeName =
         * EntityTypesList.get( ( new Random() ).nextInt( EntityTypesList.size() ) ); // Make request Stopwatch
         * stopwatch = Stopwatch.createStarted(); Iterable<Multimap<FullQualifiedName, Object>> result = dataService
         * .readAllEntitiesOfType( new FullQualifiedName( NAMESPACE, entityTypeName ) ); // print result
         * stopwatch.stop();
         *
         * totalTime += stopwatch.elapsed( TimeUnit.MILLISECONDS );
         *
         * bwAll.write( i + "," + stopwatch.elapsed( TimeUnit.MILLISECONDS ) + " \n" ); }
         *
         * bwAverage.write( "Number of tests: " + numTest + " \n" ); bwAverage.write( "Average Time (ms):" + totalTime /
         * numTest + " \n" );
         *
         * bwAll.close(); bwAverage.close();
         */
    }

    // @BeforeClass
    public static void PopulateWithData() throws Exception {
        // Perhaps drop keyspace to make things cleaner
        loadDefaultPropertyTypes();
        generatePropertyTypes( 20 );
        // Add in a key column that will be used as partition key
        propertyTypesList.add(
                new CustomPropertyType( "key", EdmPrimitiveTypeKind.Int32, "key", () -> ++partitionKey, "Integer" ) );

        try {
            generateCSV( 20000, "src/test/resources/stressTest.csv" );
        } catch ( Exception e ) {
            e.printStackTrace();
        }

        ds.sprout( "cassandra", "local" );
        dms = ds.getContext().getBean( EdmManager.class );
        odsc = ds.getContext().getBean( ODataStorageService.class );
        cdm = ds.getContext().getBean( HazelcastEntityDatastore.class );

        // Create PropertyType, Entity Types, Entity Sets in database
        createPropertyTypes();
        // Create 3 EntityTypes, each with 2 EntitySets for this test in database
        createEntityTypes( 3, 2 );
        createSchema();

        writeCSVToDB( "src/test/resources/stressTest.csv" );

        System.out.println( "TEST STARTS" );
    }

    // @AfterClass
    public static void PlowingUnder() {
        ds.plowUnder();
        System.out.println( "TEST DONE" );
    }

    /**
     * Custom PropertyType. Will use to generate PropertyType in datastore.
     *
     * WARNING: THIS TEST IS DISABLED FOR NOW.
     *
     * @author Ho Chung Siu
     */
    private static class CustomPropertyType {
        private UUID                 id;
        private String               name;
        private EdmPrimitiveTypeKind dataType;
        private String               keyword;
        private Callable             randomGenCallable;
        private String               javaTypeName;

        public CustomPropertyType(
                UUID id,
                String name,
                EdmPrimitiveTypeKind dataType,
                String keyword,
                Callable randomGenCallable,
                String javaTypeName ) {
            this.id = id;
            this.name = name;
            this.dataType = dataType;
            this.keyword = keyword;
            this.randomGenCallable = randomGenCallable;
            this.javaTypeName = javaTypeName;
        }

        public CustomPropertyType(
                String name,
                EdmPrimitiveTypeKind dataType,
                String keyword,
                Callable randomGenCallable,
                String javaTypeName ) {
            this.id = UUID.randomUUID();
            this.name = name;
            this.dataType = dataType;
            this.keyword = keyword;
            this.randomGenCallable = randomGenCallable;
            this.javaTypeName = javaTypeName;
        }

        public UUID getId() {
            return id;
        }

        public String getJavaTypeName() {
            return javaTypeName;
        }

        public String getKeyword() {
            return keyword;
        }

        public String getName() {
            return name;
        }

        public EdmPrimitiveTypeKind getDataType() {
            return dataType;
        }

        public Callable getRandomGenCallable() {
            return randomGenCallable;
        }

        public Object getRandom() throws Exception {
            return randomGenCallable.call();
        }

        public FullQualifiedName getFqn() {
            return new FullQualifiedName( NAMESPACE, name );
        }
    }
}
