/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.claro.nifi.processors.mediacionprocesadorarchetype;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.dbcp.DBCPService;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.lang.Object;


@Tags({"mediación", "claro", "record", "dba"})
@CapabilityDescription("")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})

public class MyProcessor extends AbstractProcessor {
    Mediacion mediacion = new Mediacion();

    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();
    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name( "Reader" )
            .displayName( "Record Reader" )
            .identifiesControllerService( RecordReaderFactory.class )
            .required( true )
            .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name( "Writer" )
            .displayName( "Record Writer" )
            .identifiesControllerService( RecordSetWriterFactory.class )
            .required( true )
            .build();
    static final PropertyDescriptor MYPROPERTY = new PropertyDescriptor.Builder()
            .name( "property" )
            .displayName( "property" )
            .description( "my property" )
            .required( true )
            .defaultValue( "Default Value" )
            .addValidator( StandardValidators.NON_EMPTY_VALIDATOR )
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name( "success" )
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name( "failure" )
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add( RECORD_READER );
        descriptors.add( RECORD_WRITER );
        descriptors.add( MYPROPERTY );
        descriptors.add(DBCP_SERVICE);
        this.descriptors = Collections.unmodifiableList( descriptors );

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add( REL_SUCCESS );
        relationships.add( REL_FAILURE );
        this.relationships = Collections.unmodifiableSet( relationships );
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    private DBCPService dbcpService;

    @OnScheduled
    public void setup(ProcessContext context) {
        dbcpService =  context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
    }
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final Connection con = dbcpService.getConnection( flowFile.getAttributes() );
        final RecordReaderFactory factory = context.getProperty( RECORD_READER ).asControllerService( RecordReaderFactory.class );
        final RecordSetWriterFactory writerFactoryBill = context.getProperty( RECORD_WRITER).asControllerService( RecordSetWriterFactory.class );
        final String myProperty = context.getProperty( MYPROPERTY ).getValue();

        final ComponentLog logger = getLogger();

        final FlowFile outFlowFile = session.create( flowFile );

            try {
                session.read( flowFile, in -> {
                    try (RecordReader reader = factory.createRecordReader( flowFile, in, logger )) {
                        final RecordSchema writeSchemaBill = writerFactoryBill.getSchema( null, reader.getSchema() );

                    Record record;
                    final OutputStream outStreamBil = session.write( outFlowFile );
                    final RecordSetWriter writer= writerFactoryBill.createWriter( getLogger(), writeSchemaBill, outStreamBil );
                    writer.beginRecordSet();
                    while ((record = reader.nextRecord()) != null) {
                        //########################################################
                        record.setValue( "E_Formatted_Date", mediacion.timeStampMillis());
                        record.setValue( "O_Tap_Label", "XXX" + mediacion.julian() );
                        boolean result = findDB( con, "select * from dual" );
                        record.setValue( "E_Find_DB", result );
                        writer.write( record );
                        //######################################################
                    }
                    final WriteResult writeResult = writer.finishRecordSet();
                    writer.close();

                    final Map<String, String> attributesBilling = new HashMap<>();
                    attributesBilling.put( "record.count", String.valueOf( writeResult.getRecordCount() ) );
                    attributesBilling.put( CoreAttributes.MIME_TYPE.key(), writer.getMimeType() );
                    session.putAllAttributes( outFlowFile, attributesBilling );
                    session.transfer( outFlowFile, REL_SUCCESS );

                } catch (final SchemaNotFoundException | MalformedRecordException e) {
                    getLogger().error( "SchemaNotFound or MalformedRecordException \n" + e.toString() );
                    throw new ProcessException( e );
                } catch (final Exception e) {
                    getLogger().error( e.toString() );
                    throw new ProcessException( e );
                }
            } );
        } catch (final Exception e) {
            session.transfer( flowFile, REL_FAILURE );
            logger.error( "Unable to communicate with cache when processing {} due to {}", new Object[]{flowFile, e} );
            return;
        }
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        session.remove( flowFile );
    }
    private boolean findDB(final Connection con,String sql) throws SQLException {
        int nrOfRows = 0;
        PreparedStatement ps = con.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            nrOfRows++;
        }
        ps.close();
        return nrOfRows > 0;
    }

}

