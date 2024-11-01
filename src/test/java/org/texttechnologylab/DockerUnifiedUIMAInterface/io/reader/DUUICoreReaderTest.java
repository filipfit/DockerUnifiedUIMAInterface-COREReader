package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader;

import com.opencsv.exceptions.CsvValidationException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.XmlCasSerializer;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

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
    public void testExtractByValue() throws Exception {
        TSVTable t = new TSVTable("./testTables/screenshotsTable.tsv");
        t = t.extractByValue("page_id", "24111");
        t.print();
    }

    @Test
    public void testToJcas() throws Exception {
        DUUICoreReader reader = new DUUICoreReader();

        JCas jcas = JCasFactory.createJCas();
        reader.getNextCas(jcas);

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

    @Test
    public void testHasNext() throws Exception {
        DUUICoreReader reader = new DUUICoreReader();
        JCas jcas = JCasFactory.createJCas();
        System.out.println("testHasNext temporaryPageIds.size: " + reader.pageIDs.size());

        System.out.println("testHasNext: " + reader.hasNext() + " atIndex: " + reader.nextIndex);
        reader.getNextCas(jcas);
        System.out.println("testHasNext: " + reader.hasNext() + " atIndex: " + reader.nextIndex);
        reader.getNextCas(jcas);
        System.out.println("testHasNext: " + reader.hasNext() + " atIndex: " + reader.nextIndex);
        reader.getNextCas(jcas);
        System.out.println("testHasNext: " + reader.hasNext() + " atIndex: " + reader.nextIndex);
        reader.getNextCas(jcas);
    }

    @Test
    public void testGetAllPageIDs() throws Exception {
        List<String> pageIDs = new TSVTable("TEMP_files/testTables/pageTable.tsv").getColumn(0);
        System.out.println(pageIDs);
    }

    @Test
    public void testMapUsersToPages() throws Exception {
        TSVTable pages = new TSVTable("TEMP_files/testTables/pageTable.tsv");
        TSVTable sessions = new TSVTable("TEMP_files/testTables/sessionsTable.tsv");
        List<String> pageIDs = pages.getColumn(0);
        List<List<String>> pageSessionUser = new ArrayList<>();

        for (var id : pageIDs) {
            String sessionID = pages.extractByValue("id", id).getCell("session_id", 0);
            String userID = sessions.extractByValue("id", sessionID).getCell("user_id", 0);
            List<String> row = new ArrayList<>();
            row.add(id);
            row.add(sessionID);
            row.add(userID);
            pageSessionUser.add(row);
        }
    }

    @Test
    void testGetHTML() throws Exception {
        DUUICoreReader.getHtmlFileIDs("TEMP_files/TEMP_out/24111.xmi");
    }

    @Test void testReaderInPipeline() throws Exception {
        Path dummySource = Paths.get("./TEMP_files/TEMP_in");
        Path targetLocation = Paths.get("./TEMP_files/TEMP_out");

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
//        CollectionReaderDescription reader =
//                CollectionReaderFactory.createReaderDescription(
//                        DUUICoreReader.class,
//                        corePageTypes,
//                        DUUICoreReader.PARAM_SOURCE_LOCATION, dummySource.toString(),
//                        DUUICoreReader.PARAM_PATTERNS, "[+]*.*"
//                );

        // Reader: CoreReader

        DUUIAsynchronousProcessor reader = new DUUIAsynchronousProcessor(
                new DUUICoreReader()
        );

        // XMI Writer
        composer.add(new DUUIUIMADriver.Component(
                AnalysisEngineFactory.createEngineDescription(
                        XmiWriter.class
                        , XmiWriter.PARAM_TARGET_LOCATION, targetLocation.toString()
                        , XmiWriter.PARAM_PRETTY_PRINT, true
                        , XmiWriter.PARAM_OVERWRITE, true
                        , XmiWriter.PARAM_VERSION, "1.1"
                        , XmiWriter.PARAM_COMPRESSION, "NONE"
                )
        ));

        composer.run(reader, "core_reader_test");
        composer.shutdown();
    }
}