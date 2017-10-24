package de.yagee.oai;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.XMLConstants;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.xerces.util.XMLCatalogResolver;
import org.jdom2.transform.JDOMSource;
import org.mycore.oai.pmh.MetadataFormat;
import org.mycore.oai.pmh.OAIDataList;
import org.mycore.oai.pmh.Record;
import org.mycore.oai.pmh.harvester.Harvester;
import org.mycore.oai.pmh.harvester.HarvesterBuilder;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.URIConverter;

/**
 * Hello world!
 *
 */
public class Validator {

    @Parameter(required = true, arity = 1, description = "{OAI-PMH base URL}", converter = URIConverter.class)
    private List<URI> baseURL;

    @Parameter(names = "-set", arity = 1, description = "OAI set to harvest, optional")
    private String set;

    @Parameter(required = true,
        names = "-format",
        arity = 1,
        description = "metadata format of records, (default:only identifiers)")
    private String metadataPrefix;

    private int status;

    public static void main(String[] args) throws Exception {
        System.out.println("OAI-PMH Tester. " + Stream.of(args).collect(Collectors.toList()));
        Validator main = new Validator();
        try {
            JCommander.newBuilder().addObject(main).allowAbbreviatedOptions(true).build().parse(args);
            main.run();
        } catch (ParameterException e) {
            e.printStackTrace();
            e.getJCommander().usage();
        }
    }

    private void run() throws Exception {
        URI baseURI = baseURL.stream().findFirst()
            .map(uri -> {
                try {
                    return new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), null, null);
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                    return null;
                }
            }).get();

        System.out.print("Getting metadata definition from " + baseURI + " ...");
        Harvester harvester = HarvesterBuilder.createNewInstance(baseURI.toString());
        MetadataFormat metadataFormat = harvester.listMetadataFormats()
            .stream()
            .filter(mf -> mf.getPrefix().equals(metadataPrefix))
            .findAny()
            .orElseThrow(() -> new RuntimeException("Unsupported metadata format: " + metadataPrefix));
        String schema = metadataFormat.getSchema();
        if (schema.equals("http://schema.datacite.org/oai/oai-1.0/oai_datacite.xsd")) {
            schema = "https://schema.datacite.org/oai/oai-1.0/oai.xsd";
        }
        if (schema.equals("http://files.dnb.de/standards/xmetadissplus/xmetadissplus.xsd")) {
            schema = "http://www.d-nb.de/standards/xmetadissplus/xmetadissplus.xsd";
        }
        URI schemaURI = baseURI.resolve(schema);
        System.out.println("done");
        System.out.print("Creating schema " + schema + " ...");
        SchemaFactory schemaFactory = getSchemaFactory();
        Schema metadataSchema = schemaFactory.newSchema(schemaURI.toURL());
        javax.xml.validation.Validator metaDataValidator = metadataSchema.newValidator();
        System.out.println("done");
        OAIDataList<Record> records = harvester.listRecords(metadataPrefix, null, null, set);
        status = 0;
        validateRecords(metaDataValidator, records);
        while (records.isResumptionTokenSet()) {
            System.out.println("Get next result: " + records.getResumptionToken().getToken());
            records = harvester.listRecords(records.getResumptionToken().getToken());
            validateRecords(metaDataValidator, records);
        }
        System.exit(status);
    }

    private SchemaFactory getSchemaFactory() throws IOException {
        SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Enumeration<URL> catalogs = this.getClass().getClassLoader().getResources("catalog.xml");
        String[] catalogs2 = Collections.list(catalogs).stream().map(URL::toString).toArray(i -> new String[i]);
        XMLCatalogResolver resolver = new XMLCatalogResolver(catalogs2);
        schemaFactory.setResourceResolver(resolver);
        return schemaFactory;
    }

    private void validateRecords(javax.xml.validation.Validator metaDataValidator, OAIDataList<Record> records) {
        records.stream()
            .map(r -> validate(r, metaDataValidator))
            .filter(ValidationResult::failed)
            .forEach(this::handleValidationResult);
    }

    private void handleValidationResult(ValidationResult result) {
        if (result.failed()) {
            status = 1;
            result.validationException.getMessage();
        }
    }

    ValidationResult validate(Record record, javax.xml.validation.Validator validator) {
        String id = record.getHeader().getId();
        Exception validationException = null;
        try {
            validator.validate(new JDOMSource(record.getMetadata().toXML()));
        } catch (Exception e) {
            validationException = e;
        }
        return new ValidationResult(id, validationException);
    }

    private static class ValidationResult {
        private String id;

        private Exception validationException;

        public ValidationResult(String id, Exception validationException) {
            this.id = id;
            this.validationException = validationException;
        }

        public boolean isValid() {
            return validationException == null;
        }

        public boolean failed() {
            return !isValid();
        }

        public String getId() {
            return id;
        }

        public Exception getValidationException() {
            return validationException;
        }

        @Override
        public String toString() {
            if (isValid()) {
                return "Record " + getId() + " is valid.";
            }
            return "Record " + getId() + " is not valid: " + validationException.toString();
        }

    }

}
