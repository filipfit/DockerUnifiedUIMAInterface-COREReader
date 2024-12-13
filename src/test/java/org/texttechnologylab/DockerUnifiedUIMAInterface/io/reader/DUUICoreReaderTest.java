package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.XmlCasSerializer;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;

class DUUICoreReaderTest {
    @Test
    public void testHello() {
        System.out.println("Hello World!");
    }

    @Test
    public void testCreateTable() throws CsvValidationException, IOException {
        TSVTable myTable = new TSVTable("./testTables/screenshots.tsv");
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
        TSVTable t = new TSVTable("./testTables/screenshots.tsv");
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
        List<String> pageIDs = new TSVTable("TEMP_files/testTables/pages.tsv").getColumn(0);
        System.out.println(pageIDs);
    }

    @Test
    public void testMapUsersToPages() throws Exception {
        TSVTable pages = new TSVTable("TEMP_files/testTables/pages.tsv");
        TSVTable sessions = new TSVTable("TEMP_files/testTables/sessions.tsv");
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
    public void testMapPagesSessionsUsers() throws Exception{
        DUUICoreReader reader = new DUUICoreReader();
        List<List<String>> result = reader.getMappedPagesSessionsUsers();
        System.out.println(result);
    }

    @Test
    public void testGetPageID() throws Exception {
        DUUICoreReader reader = new DUUICoreReader();
        String session = reader.getSessionID("24111");
        System.out.println("pageID: 24111   sessionID: " + session);
    }

    @Test
    public void testGetUserID() throws Exception {
        DUUICoreReader reader = new DUUICoreReader();
        String user = reader.getUserID("24111");
        System.out.println(user);
    }

    @Test
    void testGetHTML() throws Exception {
        DUUICoreReader.getHtmlFileIDs("TEMP_files/TEMP_out/24111.xmi");
    }

    @Test
    void testFileSearch() throws Exception {
        TSVTable screenshotsTable = new TSVTable("TEMP_files/testTables/screenshots.tsv");
        TSVTable pageScreenshotData = screenshotsTable.extractByValue("page_id", "24111");
        Map<String, List<String>> tableMap = pageScreenshotData.getTableMap();

        Set<String> filesNames = Set.copyOf(tableMap.get("id"));
        Map<String, Path> filePaths =  CorePageUtils.filesSearch(filesNames, Paths.get("TEMP_files/TEMP_data/screens"));
        for (var entry : filePaths.entrySet()) {
//            System.out.println(entry.getKey() + ":     " + entry.getValue().toString());
            System.out.println(entry.getValue().toString());

        }
    }

    @Test
    void testToBase64() throws Exception {
        Path imgPath = Paths.get("TEMP_files/TEMP_data/screens/4537/448537.png.gz");
        Base64.Encoder encoder = Base64.getEncoder();
        String outFilePath = "TEMP_files/TEMP_out/decoded-img.png";
        String base64String;

        // Encoding png to base64
        try (InputStream stream = new GZIPInputStream(Files.newInputStream(imgPath))) {
            byte[] data = stream.readAllBytes();
            base64String = encoder.encodeToString(data);
        }
        System.out.println(base64String);

        // Decoding from base64 to png
        byte[] imageBytes = Base64.getDecoder().decode(base64String);

        try (FileOutputStream fout = new FileOutputStream(outFilePath)) {
            fout.write(imageBytes);
        }
    }

    @Test
    void dockerEasyocrTest() {
        System.out.println("docker test");
        DefaultDockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost("unix:///var/run/docker.sock") // Change if needed
                .build();

        // Docker client with explicit HTTP client
        DockerClient dockerClient = DockerClientBuilder
                .getInstance(config)
                .build();

        try {
            String containerName = "easocr-container";
            List<Container> containers = dockerClient.listContainersCmd()
                    .withShowAll(true)
                    .withNameFilter(List.of(containerName))
                    .exec();

            if (containers.isEmpty()) {
                System.out.println("Container does not exist: " + containerName);
                System.out.println("Creating container...");

                CreateContainerResponse containerResponse = dockerClient.createContainerCmd("easyocr-rest")
                        .withHostConfig(
                                HostConfig.newHostConfig()
                                        .withPortBindings(
                                                new PortBinding(Ports.Binding.bindPort(5000), new ExposedPort(5000))
                                        )
                                        .withDeviceRequests(
                                                List.of(
                                                        new DeviceRequest()
                                                                .withCapabilities(List.of(List.of("gpu")))
                                                                .withCount(-1)
                                                )
                                        )
                        )
                        .exec();

                System.out.println("Container created with ID: " + containerResponse.getId());
                containers = dockerClient.listContainersCmd()
                        .withShowAll(true)
                        .withIdFilter(List.of(containerResponse.getId()))
                        .exec();
            }

            for (var c : containers) {
                System.out.println("Container " + c.getNames().toString()  + "starting");
                dockerClient.startContainerCmd(c.getId()).exec();
                System.out.println("Status: " + c.getStatus());
            }
            dockerClient.close();

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testComposerEasyocr() throws Exception {
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(1)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIDockerDriver driver = new DUUIDockerDriver();
        composer.addDriver(driver);
        composer.add(new DUUIDockerDriver.Component("easyocr-rest").build());
//        driver.

        JCas jcas = JCasFactory.createJCas();
        jcas.setDocumentLanguage("en");
        jcas.setDocumentText("Text to process");

        composer.run(jcas, "processing-run");

    }

    @Test
    void testRequestImageTextBoundingBoxes() {}

    @Test
    void testReadGzippedHTML() throws Exception {
        Path filePath = Paths.get("TEMP_files/TEMP_data/html/7053/1945217.html.gz");
        StringBuilder htmlString = new StringBuilder();

        try (FileInputStream fis = new FileInputStream(filePath.toString());
             GZIPInputStream gis = new GZIPInputStream(fis);
             InputStreamReader isr = new InputStreamReader(gis);
             BufferedReader br = new BufferedReader(isr))
        {
            String line;
            while ((line = br.readLine()) != null) {
                htmlString.append(line).append("\n");
            }
        }

        System.out.println(htmlString.toString());
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