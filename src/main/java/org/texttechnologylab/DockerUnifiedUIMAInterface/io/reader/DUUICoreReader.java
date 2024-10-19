package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.descriptor.TypeCapability;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;

import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.AdvancedProgressMeter;
import org.texttechnologylab.annotation.corepagetypes.Screenshot;


import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

class TSVTable {
    private String filePath;
    public List<String> headers;
    public List<List<String>> table;
    public Map<String, List<String>> tableMap;

    public Map<String, List<String>> getTableMap() {
        if (tableMap != null) return tableMap;
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

    public TSVTable(List<List<String>> table) {
        List<List<String>> newTable = new ArrayList<>(table);
        this.headers = newTable.get(0);
        newTable.remove(0);
        this.table = newTable;
    }

    public List<String> getRow(Integer rowIndex) {
        return new ArrayList<>(table.get(rowIndex));
    }

    public List<Integer> getSize() {
        Integer rows = table.size();
        Integer cols = headers.size();
        return Arrays.asList(rows, cols);
    }

    public TSVTable(List<String> headers, List<List<String>> table) {
        this.headers = headers;
        this.table = table;
    }

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
    public TSVTable extractByValue(String header, String value) throws Exception {
        TSVTable newTable = new TSVTable( new ArrayList<>(this.headers), new ArrayList<>());
        Integer headerIndex = this.headers.indexOf(header);

        for (List<String> row : this.table) {
            String columnValue = row.get(headerIndex);
            if (columnValue.equals(value))  newTable.addRow(row);
        }
        return newTable;
    }
}

//CollectionReader

@TypeCapability(
        outputs = { "org.texttechnologylab.annotation.corepagetypes.Screenshot" }
)
public class DUUICoreReader
//        extends JCasResourceCollectionReader_ImplBase
        implements DUUICollectionReader
{
    private List<String> temporaryPageIds = Arrays.asList("24111", "15122", "24113", "24113");
    private TSVTable table;
    private Integer atIndex = 0;
    TypeSystemDescription tsDesc;

    public DUUICoreReader() throws CsvValidationException, IOException {
        this.table = new TSVTable("testTables/screenshotsTable.tsv");
        this.tsDesc = TypeSystemDescriptionFactory
                .createTypeSystemDescriptionFromPath(
                        "src/main/resources/org/texttechnologylab/types/CorePageTypes.xml"
                );
    }

//    public DUUICoreReader(TSVTable table) throws CsvValidationException, IOException {
//        this.table = table;
//        this.tsDesc = TypeSystemDescriptionFactory
//                .createTypeSystemDescriptionFromPath(
//                        "src/main/resources/org/texttechnologylab/types/CorePageTypes.xml"
//                );
//    }

    public void toJcas(JCas jcas) throws Exception {
//        JCas jcas = JCasFactory.createJCas(tsDesc);
        System.out.println("PAGE_ID: " + this.temporaryPageIds.get(this.atIndex));

        TSVTable pageScreenshots = table.extractByValue("page_id", this.temporaryPageIds.get(this.atIndex));
        Map<String, List<String>> tableMap = pageScreenshots.getTableMap();
        pageScreenshots.print();

        for (var i = 0; i < pageScreenshots.getSize().get(0); i++) {
            Screenshot shotAnno = new Screenshot(jcas);
            shotAnno.setId(tableMap.get("id").get(i));
            shotAnno.setReason(tableMap.get("reason").get(i));
            shotAnno.setTimestamp(tableMap.get("timestamp").get(i));
            shotAnno.setPage_id(tableMap.get("page_id").get(i));
            shotAnno.setAssessment_phase_in_session_id(tableMap.get("assessment_phase_in_session_id").get(i));
            shotAnno.setSession_id(tableMap.get("session_id").get(i));


            shotAnno.addToIndexes();
        }
        this.atIndex++;
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
        return (atIndex + 1) >= temporaryPageIds.size();
    }

    @Override
    public long getSize() {
        return temporaryPageIds.size();
    }

    @Override
    public long getDone() {
        return atIndex + 1;
    }

//    @Override
    public void getNext(JCas aJCas) throws IOException, CollectionException {
        this.getNextCas(aJCas);
    }
}
