package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.CoreReader;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

// Temporarily used class for managing tab-separated-data (.tsv) files, located in ./testTables
public class TSVTable {
    private String filePath;
    public List<String> headers;
    public List<List<String>> table;
    public Map<String, List<String>> tableMap;

    /**
     * Returns a hashtable-represantation of the of this.table. The Keys are the column headers and the values are lists
     * of all values in the column.
     *
     * @return Hashtable-Representation of this.table
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
     *
     * @param filePath Path to .tsv file with data
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
     *
     * @param table 2D nested list with each String being a table-cell datum.
     */
    public TSVTable(List<List<String>> table) {
        List<List<String>> newTable = new ArrayList<>(table);
        this.headers = newTable.get(0);
        newTable.remove(0);
        this.table = newTable;
    }

    /**
     * Constrcts a TSVTable from a list of column headers and a 2D nested list of table-data.
     *
     * @param headers Column headers of the table
     * @param table   2D nested list with each String being a table-cell datum
     */
    public TSVTable(List<String> headers, List<List<String>> table) {
        this.headers = headers;
        this.table = table;
    }

    /**
     * Returns a copy of a row from the table as a list of strings.
     *
     * @param rowIndex Index of the row.
     * @return Row data as list of strings
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
     *
     * @return Number of rows and columns of this.table in an array.
     */
    public List<Integer> getSize() {
        Integer rows = table.size();
        Integer cols = headers.size();
        return Arrays.asList(rows, cols);
    }

    /**
     * Gets a single table-cell of data.
     *
     * @param header   Column header of the cell
     * @param rowIndex Row index of the cell
     * @return Single table-cell datum
     */
    public String getCell(String header, Integer rowIndex) {
        Integer colIndex = this.headers.indexOf(header);
        return this.table.get(rowIndex).get(colIndex);
    }

    /**
     * Returns the selected rows from start (inclusive) to end (exclusive) as a new TSVTable.
     * Headers are already excluded.
     *
     * @param start Start index of rows selection range
     * @param end   End index of rows selection range
     * @return New TSVTable with the selected rows and headings from the original table.
     */
    public TSVTable getRows(Integer start, Integer end) {
        List<List<String>> newTable = new ArrayList<>(this.table.subList(start, end));
        return new TSVTable(new ArrayList<>(this.headers), newTable);
    }

    /**
     * @param row
     * @return
     * @throws Exception
     */
    public TSVTable addRow(List<String> row) throws Exception {
        if (this.headers.size() != row.size()) {
            throw new Exception("TSVTable.addRow: Column numbers do not match!");
        }
        this.table.add(new ArrayList<>(row));
        return this;
    }

    /**
     * Prints the table
     */
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
     * @param header Column selected by header name, in which each rows value is compared to "value"
     * @param value  Value to compare to. If a row's value matches, the row is included in the returned TSVTable.
     * @return TSVTable with all the rows whose value in column "header" match "value".
     * @throws Exception
     */
    public TSVTable extractByValue(String header, String value) {
        TSVTable newTable = new TSVTable(new ArrayList<>(this.headers), new ArrayList<>());
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
