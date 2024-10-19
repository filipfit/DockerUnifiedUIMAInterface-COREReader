package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader;

import com.opencsv.exceptions.CsvValidationException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.XmlCasSerializer;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.html.google.DUUIHTMLGoogleSERPReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class DUUICoreReaderTest {
    @Test
    public void testHello() {
        System.out.println("Hello World!");
    }

    @Test
    public void testCreateTable() throws CsvValidationException, IOException {
        TSVTable myTable = new TSVTable("./testTables/screenshotsTable.tsv");
        TSVTable newTable = myTable.getRows(0, 4);

        myTable.print();
        System.out.println();
        newTable.print();
        System.out.println();
        newTable.getTableMap();
        newTable.getTableMap();
    }

    @Test
    public void testExtraction() throws Exception {
        TSVTable t = new TSVTable("./testTables/screenshotsTable.tsv");
        t = t.extractByValue("page_id", "24111");
        t.print();
    }

    @Test
    public void testToJcas() throws Exception {
//        TSVTable table = new TSVTable("testTables/screenshotsTable.tsv");
        DUUICoreReader reader = new DUUICoreReader();

        JCas jcas = JCasFactory.createJCas(reader.tsDesc);
        reader.getNext(jcas);

        // Create annotated output xmi file
        File xmiFile = new File("TEMP_out/24111.xmi");
        try(FileOutputStream out = new FileOutputStream(xmiFile)) {
            XmlCasSerializer.serialize(jcas.getCas(), out);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("XMI file created: " + xmiFile.getAbsolutePath());
        System.out.println("24111");
    }

    @Test void testReaderInPipeline() throws Exception {
        Path dummySource = Paths.get("./TEMP_in");
        Path targetLocation = Paths.get("./TEMP_out");

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(1)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        composer.addDriver(uimaDriver);

//        // Typesystem: CorePageTypes.xml
//        TypeSystemDescription corePageTypes = TypeSystemDescriptionFactory
//                .createTypeSystemDescriptionFromPath(
//                        "src/main/resources/org/texttechnologylab/types/CorePageTypes.xml"
//                );

        // Reader: CoreReader
        DUUIAsynchronousProcessor reader = new DUUIAsynchronousProcessor(
                new DUUICoreReader(),
                new DUUICoreReader()
        );

//        CollectionReaderDescription reader =
//                CollectionReaderFactory.createReaderDescription(
//                        DUUICoreReader.class,
//                        corePageTypes,
//                        DUUICoreReader.PARAM_SOURCE_LOCATION, dummySource.toString(),
//                        DUUICoreReader.PARAM_PATTERNS, "[+]*.*"
//                );

        // XMI Writer
        composer.add(new DUUIUIMADriver.Component(
                AnalysisEngineFactory.createEngineDescription(
                        XmiWriter.class
                        , XmiWriter.PARAM_TARGET_LOCATION, targetLocation.toString()
                        , XmiWriter.PARAM_PRETTY_PRINT, true
                        , XmiWriter.PARAM_OVERWRITE, true
                        , XmiWriter.PARAM_VERSION, "1.1"
                        , XmiWriter.PARAM_COMPRESSION, "GZIP"
                )
        ));

        composer.run(reader, "core_reader_test");
        composer.shutdown();
    }

}