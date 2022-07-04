package m.csv.avro;

import cdr.types.Reading;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.*;

public class MCsvToAvroProcessor extends AbstractProcessor {

    private FlowFile flowFile;
    private String line;

    public static final PropertyDescriptor CSV_DELIMITER = new PropertyDescriptor.Builder()
            .name("CSV DELIMITER")
            .displayName("csv delimiter")
            .description("set your csv delimiter.")
            .required(true)
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    // 1 - SETTINGS Success
    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("Success")
            .description("Csv file converted to Avro successfully")
            .build();

    // 2 - SETTINGS Failure
    public static final Relationship ERROR_RELATIONSHIP = new Relationship.Builder()
            .name("Failure")
            .description("Csv file processed with errors")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);

        final ArrayList<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CSV_DELIMITER);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(ERROR_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }
    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        String fileName = flowFile.getAttribute("filename");
        long timestamp = new Timestamp(System.currentTimeMillis()).getTime();

        //session.putAttribute(flowFile, "filename", fileName.substring(0, fileName.lastIndexOf('.')) + "h_" + timestamp + ".avro");

        flowFile = session.write(flowFile, (inputStream, outputStream) -> {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

            final DatumWriter<Reading> datumWriter = new SpecificDatumWriter<>(Reading.class);
            try (DataFileWriter<Reading> dataFileWriter = new DataFileWriter<>(datumWriter)) {
                String csv_delimiter = context.getProperty(CSV_DELIMITER).getValue();
                Reading.Builder cdrBuilder = Reading.newBuilder();
                Reading cdr = new Reading();
                dataFileWriter.create(cdr.getSchema(), outputStream);
                while ((line = bufferedReader.readLine()) != null) {
                    String[] arr = line.split(csv_delimiter);
                    cdrBuilder.setLastUpdate(Long.parseLong(arr[2]));
                    cdrBuilder.setMeterSerial(arr[3]);
                    cdrBuilder.setReading(Float.parseFloat(arr[5]));
                    cdr = cdrBuilder.build();
                    dataFileWriter.append(cdr);
                }
            } catch (IOException ex) {
                logger.error("\nPROCESSOR ERROR 1: " + ex.getMessage() + "\n");
                bufferedReader.close();
                session.transfer(flowFile, ERROR_RELATIONSHIP);
                return;
            }

            logger.info("Successfully transfer file :)");
            bufferedReader.close();
        });
        session.transfer(flowFile, SUCCESS_RELATIONSHIP);
    }
}
