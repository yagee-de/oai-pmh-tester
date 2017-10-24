package de.yagee.oai;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.namespace.NamespaceContext;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.mycore.oai.pmh.harvester.DataProviderConnection;
import org.xml.sax.InputSource;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.URIConverter;
import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;

/**
 * Hello world!
 *
 */
public class App {

    @Parameter(required = true, arity = 1, description = "{OAI-PMH base URL}", converter = URIConverter.class)
    private List<URI> baseURL;

    @Parameter(names = "-set", arity = 1, description = "OAI set to harvest, optional")
    private String set;

    @Parameter(names = "-format", arity = 1, description = "metadata format of records, (default:only identifiers)")
    private String metadataPrefix;

    public static void main(String[] args) throws Exception {
        System.out.println("OAI-PMH Tester. " + Stream.of(args).collect(Collectors.toList()));
        App main = new App();
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
        DataProviderConnection dataProviderConnection = new DataProviderConnection(baseURI.toString());
        XPath xPath = XPathFactory.newInstance().newXPath();
        xPath.setNamespaceContext(getOAINameSpaceContext());
        XPathExpression rtExpr = metadataPrefix == null
            ? xPath.compile("/oai:OAI-PMH/oai:ListIdentifiers/oai:resumptionToken")
            : xPath.compile("/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken");
        try (ByteArrayOutputStream bout = new ByteArrayOutputStream(128 * 1024 * 1024)) {
            System.out.printf("%s,", baseURI);
            Optional<String> oRt = fetchResult(
                () -> metadataPrefix == null ? dataProviderConnection.listIdentifiers("oai_dc", null, null, set)
                    : dataProviderConnection.listRecords(metadataPrefix, null, null, set),
                bout, rtExpr);
            while (oRt.isPresent()) {
                String rt = oRt.get();
                System.out.printf("%s,", rt);
                bout.reset();
                oRt = fetchResult(() -> metadataPrefix == null ? dataProviderConnection.listIdentifiers(rt)
                    : dataProviderConnection.listRecords(rt), bout, rtExpr);
            }
        }
    }

    private static NamespaceContext getOAINameSpaceContext() {
        return new NamespaceContext() {

            @Override
            public Iterator<String> getPrefixes(String namespaceURI) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getPrefix(String namespaceURI) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getNamespaceURI(String prefix) {
                return "http://www.openarchives.org/OAI/2.0/";
            }
        };
    }

    private static Optional<String> fetchResult(Supplier<InputStream> resultProvider, ByteArrayOutputStream bout,
        XPathExpression xe) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try (InputStream is = resultProvider.get()) {
//            System.out.printf("%d,", stopwatch.elapsed(TimeUnit.MILLISECONDS));
            ByteStreams.copy(is, bout);
        }
        //        System.out.println(new String(bout.toByteArray()));
        stopwatch.stop();
        String resumptionToken = null;
        try (InputStream is = new ByteArrayInputStream(bout.toByteArray())) {
            resumptionToken = xe.evaluate(new InputSource(is));
            if (resumptionToken != null && resumptionToken.trim().isEmpty()) {
                resumptionToken = null;
            }
        }
        System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return Optional.ofNullable(resumptionToken);
    }
}
