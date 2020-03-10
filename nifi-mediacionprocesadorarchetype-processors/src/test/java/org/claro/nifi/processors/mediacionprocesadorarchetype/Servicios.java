package org.claro.nifi.processors.mediacionprocesadorarchetype;

import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.csv.CSVRecordSetWriter;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.dbcp.DBCPConnectionPool;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.TestRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Servicios<runner> {

    public static void  ConDB (TestRunner runner) throws InitializationException {
        final DBCPConnectionPool service = new DBCPConnectionPool();
        runner.addControllerService("Data Base", service);

        runner.setProperty(service, DBCPConnectionPool.DATABASE_URL, "jdbc:oracle:thin:@bengolea.claro.amx:1521:ARDSPB1");
        runner.setProperty(service, DBCPConnectionPool.DB_USER, "STL");
        runner.setProperty(service, DBCPConnectionPool.DB_PASSWORD, "stl");
        runner.setProperty(service, DBCPConnectionPool.DB_DRIVERNAME, "oracle.jdbc.OracleDriver");
        runner.setProperty(service, DBCPConnectionPool.DB_DRIVER_LOCATION, "C:\\Users\\CTI8820\\Desktop\\Nifi\\nifi-1.9.0\\lib\\ojdbc8.jar");
        runner.setProperty(service, DBCPConnectionPool.MAX_WAIT_TIME, "500 millis");

        runner.enableControllerService(service);
        runner.assertValid(service);
    }


    public static void CSVReader(TestRunner runner) throws IOException, InitializationException {
        //Reader
        final CSVReader csvReader = new CSVReader();
        final String inputSchemaText = new String( Files.readAllBytes( Paths.get( "src/test/inschema.avsc") ) );
        runner.addControllerService( "reader", csvReader );

        runner.setProperty( csvReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY );
        runner.setProperty( csvReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText );
        runner.setProperty( csvReader, CSVUtils.VALUE_SEPARATOR, "|" );
        runner.setProperty( csvReader, CSVUtils.FIRST_LINE_IS_HEADER, "false" );
        runner.setProperty( csvReader, CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_MINIMAL.getValue() );
        runner.setProperty( csvReader, CSVUtils.TRAILING_DELIMITER, "false" );

        runner.enableControllerService( csvReader );
    }

    public static void  CSVWriter(TestRunner runner)  throws InitializationException, IOException {
        //Writer
        final CSVRecordSetWriter csvWriter = new CSVRecordSetWriter();
        final String outputSchemaRawdata = new String( Files.readAllBytes( Paths.get( "src/test/outschema.avsc") ) );
        runner.addControllerService( "writer", csvWriter );

        runner.setProperty( csvWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY );
        runner.setProperty( csvWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaRawdata );
        runner.setProperty( csvWriter, "Schema Write Strategy", "full-schema-attribute" );
        runner.setProperty( csvWriter, CSVUtils.INCLUDE_HEADER_LINE, "false" );
        runner.setProperty( csvWriter, CSVUtils.VALUE_SEPARATOR, "|" );
        runner.setProperty( csvWriter, CSVUtils.TRAILING_DELIMITER, "false" );

        runner.enableControllerService( csvWriter );
    }

    public static void JsonReader(TestRunner runner) throws IOException, InitializationException {
        //Reader
        final JsonTreeReader jsonReader = new JsonTreeReader();
        //final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/inschema.avsc")));
        runner.addControllerService( "reader", jsonReader );
        //runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        //runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService( jsonReader );
    }
    public static void JsonWriter(TestRunner runner) throws IOException, InitializationException {
        //Writer
        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        //final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/outschema.avsc")));
        runner.addControllerService("writer", jsonWriter);
        //runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        //runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        //runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);
    }

}
