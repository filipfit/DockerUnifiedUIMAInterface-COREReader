package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.descriptor.TypeCapability;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;

import org.apache.uima.util.XmlCasDeserializer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.AdvancedProgressMeter;
import org.texttechnologylab.annotation.corepagetypes.*;
import org.xml.sax.SAXException;


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;


// Temporarily used class for managin tab-separated-data (.tsv) files, located in ./testTables
class TSVTable {
    private String filePath;
    public List<String> headers;
    public List<List<String>> table;
    public Map<String, List<String>> tableMap;

    /**
     * Returns a hashtable-represantation of the of this.table. The Keys are the column headers and the values are lists
     * of all values in the column.
     * @return  Hashtable-Representation of this.table
     */
    public Map<String, List<String>> getTableMap() {
        if (this.tableMap != null) return this.tableMap;
        Map<String, List<String>> tableMap = new HashMap<>();
        Integer columnCount = this.headers.size();
        Integer rowCount = this.table.size();

        for (String h : this.headers) {
            tableMap.put(h, new ArrayList<>());
        }

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) { // Ignore headings row
            List<String> row = this.table.get(rowIndex);
            for (int colIndex = 0; colIndex < columnCount; colIndex++) {
                tableMap.get(this.headers.get(colIndex)).add(row.get(colIndex));
            }
        }

        this.tableMap = tableMap;
//        for (var entry : this.tableMap.entrySet()) {
//            System.out.println(entry.getKey() + ":" + entry.getValue().toString());
//        }
        return this.tableMap;
    }

    /**
     * Constructs a TSVTable from the .tsv file in "filepath".
     * @param filePath  Path to .tsv file with data
     * @throws IOException
     * @throws CsvValidationException
     */
    public TSVTable(String filePath) throws IOException, CsvValidationException {
        this.table = new ArrayList<>();
        this.filePath = filePath;
        CSVParser parser = new CSVParserBuilder().withSeparator('\t').build();
        CSVReader reader = new CSVReaderBuilder(new FileReader(filePath))
                .withCSVParser(parser)
                .build();

        this.headers = Arrays.asList(reader.readNext());
        String[] line;
        while ((line = reader.readNext()) != null) {
            List<String> rowElements = Arrays.asList(line);
            this.table.add(rowElements);
        }
    }

    /**
     * Constructs a TSVTable from a 2D  nested list. Each nested list is considered a row. Each String is considered
     * data in a table-cell. The first nested list should contain the column headers.
     * @param table     2D nested list with each String being a table-cell datum.
     */
    public TSVTable(List<List<String>> table) {
        List<List<String>> newTable = new ArrayList<>(table);
        this.headers = newTable.get(0);
        newTable.remove(0);
        this.table = newTable;
    }

    /**
     * Constrcts a TSVTable from a list of column headers and a 2D nested list of table-data.
     * @param headers   Column headers of the table
     * @param table     2D nested list with each String being a table-cell datum
     */
    public TSVTable(List<String> headers, List<List<String>> table) {
        this.headers = headers;
        this.table = table;
    }

    /**
     * Returns a copy of a row from the table as a list of strings.
     * @param rowIndex      Index of the row.
     * @return              Row data as list of strings
     */
    public TSVTable getRow(Integer rowIndex) {
        List<List<String>> row = new ArrayList<>();
        row.add(this.table.get(rowIndex));
        return new TSVTable(this.headers, row);
    }

    public List<String> getColumn(Integer columnIndex) {
        var tableMap = this.getTableMap();
        return tableMap.get(this.headers.get(columnIndex));
    }

    public List<String> getColumn(String columnHeader) {
        var tableMap = this.getTableMap();
        return tableMap.get(columnHeader);
    }

    /**
     * Returns the size of the 2D array of table data as 2 Integers in an array as such:
     * {number-of-rows, number-of-columns}
     * @return      Number of rows and columns of this.table in an array.
     */
    public List<Integer> getSize() {
        Integer rows = table.size();
        Integer cols = headers.size();
        return Arrays.asList(rows, cols);
    }

    /**
     * Gets a single table-cell of data.
     * @param header    Column header of the cell
     * @param rowIndex  Row index of the cell
     * @return          Single table-cell datum
     */
    public String getCell(String header, Integer rowIndex) {
        Integer colIndex = this.headers.indexOf(header);
        return this.table.get(rowIndex).get(colIndex);
    }

    /**
     * Returns the selected rows from start (inclusive) to end (exclusive) as a new TSVTable.
     * Headers are already excluded.
     * @param start     Start index of rows selection range
     * @param end       End index of rows selection range
     * @return          New TSVTable with the selected rows and headings from the original table.
     */
    public TSVTable getRows(Integer start, Integer end) {
        List<List<String>> newTable = new ArrayList<>(this.table.subList(start, end));
        return new TSVTable(new ArrayList<>(this.headers), newTable);
    }

    /**
     * 
     * @param row
     * @return
     * @throws Exception
     */
    public TSVTable addRow(List<String> row) throws Exception {
        if (this.headers.size() != row.size()) { throw new Exception("TSVTable.addRow: Column numbers do not match!"); }
        this.table.add(new ArrayList<>(row));
        return this;
    }

    public void print() {
        String headers = this.headers.stream().collect(Collectors.joining("\t"));
        System.out.println(headers);
        for (List<String> row : this.table) {
            String line = row.stream().collect(Collectors.joining("\t"));
            System.out.println(line);
        }
    }

    /**
     * Extracts rows into new table by matching value in a header column.
     *
     * @param header        Column selected by header name, in which each rows value is compared to "value"
     * @param value         Value to compare to. If a row's value matches, the row is included in the returned TSVTable.
     * @return              TSVTable with all the rows whose value in column "header" match "value".
     * @throws Exception
     */
    public TSVTable extractByValue(String header, String value) {
        TSVTable newTable = new TSVTable( new ArrayList<>(this.headers), new ArrayList<>());
        Integer headerIndex = this.headers.indexOf(header);

        try {
            for (List<String> row : this.table) {
                String columnValue = row.get(headerIndex);
                if (columnValue.equals(value)) newTable.addRow(row);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return newTable;
    }
}

class CorePageUtils {

    public static String getBaseName(String fileName) {
        int index = fileName.indexOf('.');
        return (index == -1) ? fileName : fileName.substring(0, index);
    }

    /**
     * Searches for files whose file name is in fileNames. Starts search in startDir. File names must contain only files
     * without any of their extension. E.g. "image.png.gz" should be "image".
     * @param fileNames     List of just filenames without any extensions
     * @param startDir      Directory to start searching from
     * @return              List of full paths to the found files
     * @throws IOException
     */
    public static Map<String, Path> filesSearch(Set<String> fileNames, Path startDir) throws IOException {
        return Files.walk(startDir)
                .filter(Files::isRegularFile)
                .filter(path -> {
                    String f = CorePageUtils.getBaseName(path.getFileName().toString());
                    return fileNames.contains(f);
                })
                .collect(Collectors.toMap(
                        path -> CorePageUtils.getBaseName(path.getFileName().toString()),
                        path -> path
                ));
    }

    public static String pngToBase64(Path imagePath) throws IOException {
        Base64.Encoder encoder = Base64.getEncoder();
        String base64String;

        try (InputStream stream = new GZIPInputStream(Files.newInputStream(imagePath))) {
            byte[] data = stream.readAllBytes();
            base64String = encoder.encodeToString(data);
        }
        return base64String;
    }

    public static String readGzippedHTML(Path gzippedFilePath) throws IOException {
        StringBuilder htmlString = new StringBuilder();
        try (GZIPInputStream gzStream = new GZIPInputStream(Files.newInputStream(gzippedFilePath));
            InputStreamReader reader = new InputStreamReader(gzStream, StandardCharsets.UTF_8);
            BufferedReader bufferedReader = new BufferedReader(reader)
        ) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                htmlString.append(line).append("\n");
            }
        }
        return htmlString.toString();
    }

}

@TypeCapability(
        outputs = { "org.texttechnologylab.annotation.corepagetypes.Screenshot" }
)
public class DUUICoreReader
//        extends JCasResourceCollectionReader_ImplBase
        implements DUUICollectionReader
{
    public List<String> pageIDs = new ArrayList<>();
    private TSVTable pageTable;
    private TSVTable screenshotsTable;
    private TSVTable htmlTable;
    private TSVTable scrolleventTable;
    private TSVTable sessionDataTable;
    private TSVTable userDataTable;
    private List<List<String>> mappedPagesSessionsUsers;
    // Index of the next pageID to be processed
    public Integer nextIndex = 0;
    TypeSystemDescription tsDesc;

    public DUUICoreReader() throws CsvValidationException, IOException {
        this.pageTable =        new TSVTable("TEMP_files/testTables/pageTable.tsv");
        this.screenshotsTable = new TSVTable("TEMP_files/testTables/screenshotsTable.tsv");
        this.htmlTable =        new TSVTable("TEMP_files/testTables/htmlTable.tsv");
        this.scrolleventTable = new TSVTable("TEMP_files/testTables/scrollTable.tsv");
        this.sessionDataTable = new TSVTable("TEMP_files/testTables/sessionsTable.tsv");
        this.userDataTable =    new TSVTable("TEMP_files/testTables/neobridgeUserTable.tsv");
        this.pageIDs = new TSVTable("TEMP_files/testTables/pageTable.tsv").getColumn(0);
        this.tsDesc = TypeSystemDescriptionFactory
                .createTypeSystemDescriptionFromPath(
                        "src/main/resources/org/texttechnologylab/types/CorePageTypes.xml"
                );
    }

    @Override
    public AdvancedProgressMeter getProgress() {
        return null;
    }

    @Override
    public void getNextCas(JCas pCas) {
        try {
            this.toJcas(pCas);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasNext() {
        return !(nextIndex >= pageIDs.size());
    }

    @Override
    public long getSize() {
        return pageIDs.size();
    }

    @Override
    public long getDone() {
        return nextIndex + 1;
    }

    /**
     * Returns an array as following: {pageID, sessionID, userID}
     * Pages 'pageId' > 'sessionID' ->  Sessions 'sessionID' > 'userID'
     * @return      Nested list where each list contains {pageID, sessionID, userID}
     * @throws CsvValidationException
     * @throws IOException
     */
    public List<List<String>> getMappedPagesSessionsUsers() throws CsvValidationException, IOException {
        if (this.mappedPagesSessionsUsers != null) return this.mappedPagesSessionsUsers;

        TSVTable pages = new TSVTable("TEMP_files/testTables/pageTable.tsv");
        TSVTable sessions = new TSVTable("TEMP_files/testTables/sessionsTable.tsv");
        List<List<String>> result = new ArrayList<>();

        for (var id : this.pageIDs) {
            String sessionID = pages.extractByValue("id", id).getCell("session_id", 0);
            String userID = sessions.extractByValue("id", sessionID).getCell("user_id", 0);
            List<String> row = new ArrayList<>();
            row.add(id);
            row.add(sessionID);
            row.add(userID);
            result.add(row);
        }

        this.mappedPagesSessionsUsers = result;
        return result;
    }

    public String getSessionID(String pageID) throws CsvValidationException, IOException {
        String sessionID = this.getMappedPagesSessionsUsers().stream()
                .filter(arr -> arr.get(0).equals(pageID))
                .findFirst()
                .get()
                .get(1);
        return sessionID;
    }

    public String getUserID(String pageID) throws CsvValidationException, IOException {
        String userID = this.getMappedPagesSessionsUsers().stream()
                .filter(arr -> arr.get(0).equals(pageID))
                .findFirst()
                .get()
                .get(2);
        return userID;
    }

    public void addMetadata(JCas jcas, String id) {
        DocumentMetaData dmd = new DocumentMetaData(jcas);
        dmd.setDocumentId(id);
        dmd.setDocumentTitle(id);
        dmd.addToIndexes();

    }

    public void annotatePageData(JCas jcas, String pageID) {
        Map<String, List<String>> pageDataMap = this.pageTable
                .extractByValue("id", pageID)
                .getTableMap();

        Page pageAnno = new Page(jcas);

        pageAnno.setId(pageDataMap.get("id").get(0));
        pageAnno.setTitle(pageDataMap.get("title").get(0));
        pageAnno.setUrl(pageDataMap.get("url").get(0));
        pageAnno.setSession_id(pageDataMap.get("session_id").get(0));
        pageAnno.setTab_id(pageDataMap.get("tab_id").get(0));
        pageAnno.setAssessment_phase_in_session_id(pageDataMap.get("assessment_phase_in_session_id").get(0));
        pageAnno.addToIndexes();
    }

    public void annotateSession(JCas jcas, String pageID) throws CsvValidationException, IOException {
        /*
         * From pageID find out the sessionID
         * Having sessionID get every row from sessionsTable.tsv whose id column equals the sessionID
         * A Page can only have one session associated with it
         */
        List<List<String>> mappedPagesSessions = this.getMappedPagesSessionsUsers().stream()
                .filter(arr -> arr.get(0).equals(pageID))
                .toList();
        String sessionID = mappedPagesSessions.get(0).get(1); // Nested list, second element
        // Only the first found row, because a page can only belong to one session
        Map<String, List<String>> sessionTableMap = this.sessionDataTable
                .extractByValue("id", sessionID)
                .getRow(0)
                .getTableMap();

        UserSession sessionAnno = new UserSession(jcas);
        sessionAnno.setId(sessionTableMap.get("id").get(0));
        sessionAnno.setStarted(sessionTableMap.get("started").get(0));
        sessionAnno.setUseragent(sessionTableMap.get("useragent").get(0));
        sessionAnno.setWebExtensionKey(sessionTableMap.get("webExtensionKey").get(0));
        sessionAnno.addToIndexes();
    }

    public void annotateUser(JCas jcas, String pageID) throws CsvValidationException, IOException {
        /*
         * From pageID find out the sessionID and from sessionID find out userID
         * Having the userID get every row from neobridgeUserTable.tsv whose id column equals the userID
         * A Page can only have one user associated with it
         */
        List<List<String>> mappedPagesUsers = this.getMappedPagesSessionsUsers().stream()
                .filter(arr -> arr.get(0).equals(pageID))
                .toList();
        String userID = mappedPagesUsers.get(0).get(2); // Nested list, third element
        // Only the first found row, because a page can only be associated with one user
        Map<String, List<String>> userTableMap = this.userDataTable
                .extractByValue("id", userID)
                .getRow(0)
                .getTableMap();

        NeobridgeUser userAnno = new NeobridgeUser(jcas);
        userAnno.setId(userTableMap.get("id").get(0));
        userAnno.setCreated(userTableMap.get("created").get(0));
        userAnno.setEmail(userTableMap.get("email").get(0));
        userAnno.setOpenId(userTableMap.get("openId").get(0));
        userAnno.setRoles(userTableMap.get("roles").get(0));
        userAnno.setUserID(userTableMap.get("userID").get(0));
        userAnno.setUsername(userTableMap.get("username").get(0));
        userAnno.setPicture(userTableMap.get("picture").get(0));
        userAnno.setRealName(userTableMap.get("realName").get(0));
        userAnno.addToIndexes();
    }

    public void annotateAllHtmlData(JCas jcas, String pageID) throws IOException {
        TSVTable pageHtmlData = this.htmlTable.extractByValue("page_id", pageID);
        Map<String, List<String>> tableMap = pageHtmlData.getTableMap();
        Map<String, Path> htmlSourcePaths = CorePageUtils.filesSearch(
                Set.copyOf(tableMap.get("id")),
                Paths.get("TEMP_files/TEMP_data/html")
        );

        for (var i = 0; i < pageHtmlData.getSize().get(0); i++) {
            HTMLData htmlAnno = new HTMLData(jcas);
            htmlAnno.setId(tableMap.get("id").get(i));
            htmlAnno.setSource(tableMap.get("source").get(i));
            htmlAnno.setTimestamp(tableMap.get("timestamp").get(i));

//            String htmlSource = Files.readString(htmlSourcePaths.get(tableMap.get("id").get(i)));
            String htmlSource = CorePageUtils.readGzippedHTML(htmlSourcePaths.get(tableMap.get("id").get(i)));
            htmlAnno.setHTMLSource(htmlSource);

            htmlAnno.addToIndexes();
        }
    }

    public void annotateAllScreenshotData(JCas jcas, String pageID) throws Exception {
        TSVTable pageScreenshotData = this.screenshotsTable.extractByValue("page_id", pageID);
        Map<String, List<String>> tableMap = pageScreenshotData.getTableMap();
        Map<String, Path> screenshotPaths = CorePageUtils.filesSearch(
                Set.copyOf(tableMap.get("id")),
                Paths.get("TEMP_files/TEMP_data/screens")
        );

        for (var i = 0; i < pageScreenshotData.getSize().get(0); i++) {
            Screenshot shotAnno = new Screenshot(jcas);
            shotAnno.setId(tableMap.get("id").get(i));
            shotAnno.setReason(tableMap.get("reason").get(i));
            shotAnno.setTimestamp(tableMap.get("timestamp").get(i));
            String base64Img = CorePageUtils.pngToBase64(
                    screenshotPaths.get(tableMap.get("id").get(i))
            );
            shotAnno.setBase64Encoding(base64Img);

            shotAnno.addToIndexes();
        }
    }

    public void annotateAllScrollEvents(JCas jcas, String pageID) {
        TSVTable scrollEventData = this.scrolleventTable.extractByValue("page_id", pageID);
        Map<String, List<String>> tableMap = scrollEventData.getTableMap();

            for (var i = 0; i < scrollEventData.getSize().get(0); i++) {
                ScrollEvent scrollAnno = new ScrollEvent(jcas);

                scrollAnno.setId(tableMap.get("id").get(i));
                scrollAnno.setFromX(Integer.parseInt(tableMap.get("fromX").get(i)));
                scrollAnno.setFromY(Integer.parseInt(tableMap.get("fromY").get(i)));
                scrollAnno.setToX(Integer.parseInt(tableMap.get("toX").get(i)));
                scrollAnno.setToY(Integer.parseInt(tableMap.get("toY").get(i)));
                scrollAnno.setStartTime(tableMap.get("startTime").get(i));
                scrollAnno.setEndTime(tableMap.get("endTime").get(i));
                scrollAnno.setTimestamp(tableMap.get("timestamp").get(i));
                scrollAnno.addToIndexes();
            }
    }

    public static List<String> getHtmlFileIDs (String filePath) throws ResourceInitializationException, CASException {
        File xmi = new File(filePath);
        JCas jcas = JCasFactory.createJCas();

        try (FileInputStream stream = new FileInputStream(xmi)) {
            XmlCasDeserializer.deserialize(stream, jcas.getCas(), true);
        } catch (IOException | SAXException e) {
            throw new RuntimeException(e);
        }

        for (HTMLData anno : JCasUtil.select(jcas, HTMLData.class)) {
            System.out.println("Annotation: " + anno.getId()) ;
        }

        return new ArrayList<>();
    }

    public void toJcas(JCas jcas) throws Exception {
        String currentPageID = this.pageIDs.get(this.nextIndex);

        if (JCasUtil.select(jcas, DocumentMetaData.class).isEmpty()) {
            this.addMetadata(jcas, currentPageID);
        }

        annotatePageData(jcas, currentPageID);
        annotateSession(jcas, currentPageID);
        annotateUser(jcas, currentPageID);
        annotateAllHtmlData(jcas, currentPageID);
        annotateAllScreenshotData(jcas, currentPageID);
        annotateAllScrollEvents(jcas, currentPageID);

        this.nextIndex++;
    }
}
