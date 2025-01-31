package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.CoreReader;

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
import java.nio.file.*;
import java.util.*;


/**
 * @author Filip Fitzermann
 */
@TypeCapability(
        outputs = { "org.texttechnologylab.annotation.corepagetypes.Screenshot" }
)
public class DUUICoreReader
        implements DUUICollectionReader
{
    public List<String> pageIDs = new ArrayList<>();
    // Tables of data repurposed from an SQL database export, only temporary
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
        this.pageTable =        new TSVTable("TEMP_files/testTables/pages.tsv");
        this.screenshotsTable = new TSVTable("TEMP_files/testTables/screenshots.tsv");
        this.htmlTable =        new TSVTable("TEMP_files/testTables/html_data.tsv");
        this.scrolleventTable = new TSVTable("TEMP_files/testTables/scroll_events.tsv");
        this.sessionDataTable = new TSVTable("TEMP_files/testTables/sessions.tsv");
        this.userDataTable =    new TSVTable("TEMP_files/testTables/users.tsv");
        this.pageIDs = new TSVTable("TEMP_files/testTables/pages.tsv").getColumn(0);
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

        TSVTable pages = new TSVTable("TEMP_files/testTables/pages.tsv");
        TSVTable sessions = new TSVTable("TEMP_files/testTables/sessions.tsv");
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
         * Having sessionID get every row from sessions.tsv whose id column equals the sessionID
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
         * Having the userID get every row from users.tsv whose id column equals the userID
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
