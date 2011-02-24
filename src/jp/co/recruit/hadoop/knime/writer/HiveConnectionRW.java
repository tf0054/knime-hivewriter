package jp.co.recruit.hadoop.knime.writer;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.RowIterator;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.DataContainer;
import org.knime.core.data.date.DateAndTimeCell;
import org.knime.core.data.date.DateAndTimeValue;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.DatabaseReaderConnection;
import org.knime.core.util.FileUtil;

/**
 * Copied from
 * org.knime.base.node.io.database.DBWriterConnection
 */
public final class HiveConnectionRW {

    private static final NodeLogger LOGGER =
            NodeLogger.getLogger(DatabaseReaderConnection.class);

    private DataTableSpec m_spec;

    private static DatabaseQueryConnectionSettings m_conn;

    private Statement m_stmt;

    /**
     * Creates a empty handle for a new connection.
     * @param conn a database connection object
     */
    public HiveConnectionRW(
            final DatabaseQueryConnectionSettings conn) {
        setDBQueryConnection(conn);
    }

    /**
     * Sets anew connection object.
     * @param conn the connection
     */
    public void setDBQueryConnection(
            final DatabaseQueryConnectionSettings conn) {
        m_conn = conn;
        m_spec = null;
        if (m_stmt != null) {
            try {
                m_stmt.close();
            } catch (SQLException sqle) {
                LOGGER.debug("Error while closing SQL statement, reason "
                        + sqle.getMessage());
            }
        }
        m_stmt = null;
    }

    /**
     * @return connection settings object
     */
    public DatabaseQueryConnectionSettings getQueryConnection() {
        return m_conn;
    }

    /**
     * Returns a data table spec that reflects the meta data form the database
     * result set.
     * @return data table spec
     * @throws SQLException if the connection to the database could not be
     *         established
     */
    public DataTableSpec getDataTableSpec() throws SQLException {
        if (m_spec == null || m_stmt == null) {
            try {
                /*
                final int hashAlias = System.identityHashCode(this);
                String pQuery = "SELECT * FROM (" + m_conn.getQuery()
                    + ") table_" + hashAlias + " WHERE 1 = 0";
                */
                String pQuery = m_conn.getQuery();
                
                ResultSet result = null;
                final Connection conn = m_conn.createConnection();
                try {
                    // try to see if prepared statements are supported
                    LOGGER.debug("Executing SQL statement \"" + pQuery + "\"");
                    m_stmt = conn.prepareStatement(pQuery);
                    ((PreparedStatement) m_stmt).execute();
                    m_spec = createTableSpec(
                            ((PreparedStatement) m_stmt).getMetaData());
                } catch (Exception e) {
                    LOGGER.warn("PreparedStatment not support by database: "
                            + e.getMessage(), e);
                    // otherwise use standard statement
                    m_stmt = conn.createStatement();
                    LOGGER.debug("Executing SQL statement without PreparedStatment\"" + pQuery + "\"");
                    result = m_stmt.executeQuery(pQuery);
                    m_spec = createTableSpec(result.getMetaData());
                } finally {
                    if (result != null) {
                        result.close();
                    }
                    // ensure we have a non-prepared statement to access data
                    if (m_stmt != null && m_stmt instanceof PreparedStatement) {
                        m_stmt.close();
                        m_stmt = conn.createStatement();
                    }
                }
            } catch (SQLException sql) {
                if (m_stmt != null) {
                    try {
                        m_stmt.close();
                    } catch (SQLException e) {
                        LOGGER.debug(e);
                    }
                    m_stmt = null;
                }
                throw sql;
            } catch (Throwable t) {
                throw new SQLException(t);
            }
        }
        return m_spec;
    }

    /**
     * Read data from database.
     * @param exec used for progress info
     * @return buffered data table read from database
     * @throws CanceledExecutionException if canceled in between
     * @throws SQLException if the connection could not be opened
     */
    public BufferedDataTable createTable(final ExecutionContext exec)
            throws CanceledExecutionException, SQLException {
        try {
            final DataTableSpec spec = getDataTableSpec();
            if (DatabaseConnectionSettings.FETCH_SIZE != null) {
                m_stmt.setFetchSize(
                        DatabaseConnectionSettings.FETCH_SIZE);
            } else {
                // fix 2040: mySQL databases read everything into one, big
                // ResultSet leading to an heap space error
                // Integer.MIN_VALUE is an indicator in order to enable
                // streaming results
                if (m_stmt.getClass().getCanonicalName().equals(
                        "com.mysql.jdbc.Statement")) {
                    m_stmt.setFetchSize(Integer.MIN_VALUE);
                    LOGGER.info("Database fetchsize for mySQL database set to "
                            + "\"" + Integer.MIN_VALUE + "\".");
                }
            }
            final String query = m_conn.getQuery();
            LOGGER.debug("Executing SQL statement \"" + query + "\"");
            final ResultSet result = m_stmt.executeQuery(query);
            return exec.createBufferedDataTable(new DataTable() {
                /**
                 * {@inheritDoc}
                 */
                @Override
                public DataTableSpec getDataTableSpec() {
                    return spec;
                }
                /**
                 * {@inheritDoc}
                 */
                @Override
                public RowIterator iterator() {
                    return new DBRowIterator(result);
                }

            }, exec);
        } finally {
            if (m_stmt != null) {
                m_stmt.close();
                m_stmt = null;
            }
        }
    }

    /**
     * @param cachedNoRows number of rows cached for data preview
     * @return buffered data table read from database
     * @throws SQLException if the connection could not be opened
     */
    DataTable createTable(final int cachedNoRows) throws SQLException {
        try {
            final DataTableSpec spec = getDataTableSpec();
            final String query;
            if (cachedNoRows < 0) {
                query = m_conn.getQuery();
                if (DatabaseConnectionSettings.FETCH_SIZE != null) {
                    m_stmt.setFetchSize(
                            DatabaseConnectionSettings.FETCH_SIZE);
                } else {
                    // fix 2040: mySQL databases read everything into one, big
                    // ResultSet leading to an heap space error
                    // Integer.MIN_VALUE is an indicator in order to enable
                    // streaming results
                    if (m_stmt.getClass().getCanonicalName().equals(
                            "com.mysql.jdbc.Statement")) {
                        m_stmt.setFetchSize(Integer.MIN_VALUE);
                        LOGGER.info("Database fetchsize for mySQL database "
                                + "set to \"" + Integer.MIN_VALUE + "\".");
            }
                }
            } else {
                final int hashAlias = System.identityHashCode(this);
                query = "SELECT * FROM (" + m_conn.getQuery()
                    + ") table_" + hashAlias;
                m_stmt.setMaxRows(cachedNoRows);
            }
            LOGGER.debug("Executing SQL statement \"" + query + "\"");
            m_stmt.execute(query);
            final ResultSet result = m_stmt.getResultSet();
            DBRowIterator it = new DBRowIterator(result);
            DataContainer buf = new DataContainer(spec);
            while (it.hasNext()) {
                buf.addRowToTable(it.next());
            }
            buf.close();
            return buf.getTable();
        } finally {
            if (m_stmt != null) {
                m_stmt.close();
                m_stmt = null;
            }
        }
    }

    private DataTableSpec createTableSpec(final ResultSetMetaData meta)
            throws SQLException {
        int cols = meta.getColumnCount();
        if (cols == 0) {
            return new DataTableSpec("database");
        }
        DataTableSpec spec = null;
        for (int i = 0; i < cols; i++) {
            int dbIdx = i + 1;
            String name = meta.getColumnName(dbIdx);
            int type = meta.getColumnType(dbIdx);
            DataType newType;
            switch (type) {
                // all types that can be interpreted as integer
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIT:
                case Types.BOOLEAN:
                    newType = IntCell.TYPE;
                    break;
                // all types that can be interpreted as double
                case Types.FLOAT:
                case Types.DOUBLE:
                case Types.NUMERIC:
                case Types.DECIMAL:
                case Types.REAL:
                case Types.BIGINT:
                    newType = DoubleCell.TYPE;
                    break;
                case Types.TIME:
                case Types.DATE:
                case Types.TIMESTAMP:
                    newType = DateAndTimeCell.TYPE;
                    break;
                default:
                    newType = StringCell.TYPE;
            }
            if (spec == null) {
                spec = new DataTableSpec("database",
                        new DataColumnSpecCreator(
                        name, newType).createSpec());
            } else {
                name = DataTableSpec.getUniqueColumnName(spec, name);
                spec = new DataTableSpec("database", spec,
                       new DataTableSpec(new DataColumnSpecCreator(
                               name, newType).createSpec()));
            }
        }
        return spec;
    }

    /**
	 * Create connection to write into database.
	 * @param dbConn a database connection object
	 * @param data The data to write.
	 * @param table name of table to write
	 * @param appendData if checked the data is appended to an existing table
	 * @param exec Used the cancel writing.
	 * @param sqlTypes A mapping from column name to SQL-type.
	 * @return error string or null, if non
	 * @throws Exception if connection could not be established
	 */
	static final String writeData(
	        final String table, final BufferedDataTable data,
	        final boolean appendData, final ExecutionMonitor exec,
	        final Map<String, String> sqlTypes) throws Exception {
		
	    final Connection conn = m_conn.createConnection();
	    DataTableSpec spec = data.getDataTableSpec();
	    
	    // mapping from spec columns to database columns
	    final int[] mapping;
	    // append data to existing table
	    if (appendData) {
        	LOGGER.info("writeData: appendData");

	        Statement statement = null;
	        ResultSet rs = null;
	        try {
	            // try to count all rows to see if table exists
	            final String query = "SELECT * FROM " + table;
	            LOGGER.debug("Executing SQL statement \"" + query + "\"");
	            statement = conn.createStatement();
	            rs = statement.executeQuery(query);
	        } catch (SQLException sqle) {
	            if (statement == null) {
	                throw new SQLException("Could not create SQL statement, "
	                        + "reason: " + sqle.getMessage(), sqle);
	            }
	            LOGGER.info("Table \"" + table
	                    + "\" does not exist in database, "
	                    + "will create new table.");
	            // and create new table
	            /*
	             * CREATE TABLE pref (id int, pref STRING)
    			 *  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    			 *  LINES TERMINATED BY '\n'
	             */
	            final String query = "CREATE TABLE " + table + " "
	                + createStmt(spec, sqlTypes)
	                + " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
	                + " LINES TERMINATED BY '\n'";
	            LOGGER.debug("Executing SQL statement \"" + query + "\"");
	            statement.execute(query);
	        }
	        // if table exists
	        if (rs != null) {
	            ResultSetMetaData rsmd = rs.getMetaData();
	            final Map<String, Integer> columnNames =
	                new LinkedHashMap<String, Integer>();
	            for (int i = 0; i < spec.getNumColumns(); i++) {
	                String colName = replaceColumnName(
	                        spec.getColumnSpec(i).getName()).toLowerCase();
	                columnNames.put(colName, i);
	            }
	            // sanity check to lock if all input column are present in db
	            ArrayList<String> columnNotInSpec = new ArrayList<String>(
	                    columnNames.keySet());
	            for (int i = 0; i < rsmd.getColumnCount(); i++) {
	                String colName = rsmd.getColumnName(i + 1).toLowerCase();
	                if (columnNames.containsKey(colName)) {
	                    columnNotInSpec.remove(colName);
	                }
	            }
	            if (columnNotInSpec.size() > 0) {
	                throw new RuntimeException("No. of columns in input table"
	                        + " > in database; not existing columns: "
	                        + columnNotInSpec.toString());
	            }
	            mapping = new int[rsmd.getColumnCount()];
	            for (int i = 0; i < mapping.length; i++) {
	                String name = rsmd.getColumnName(i + 1).toLowerCase();
	                if (!columnNames.containsKey(name)) {
	                    mapping[i] = -1;
	                    continue;
	                }
	                mapping[i] = columnNames.get(name);
	                DataColumnSpec cspec = spec.getColumnSpec(mapping[i]);
	                int type = rsmd.getColumnType(i + 1);
	                switch (type) {
	                    // check all int compatible types
	                    case Types.TINYINT:
	                    case Types.SMALLINT:
	                    case Types.INTEGER:
	                    case Types.BIT:
	                    case Types.BOOLEAN:
	                        // types must be compatible to IntValue
	                        if (!cspec.getType().isCompatible(IntValue.class)) {
	                            throw new RuntimeException("Column \"" + name
	                                    + "\" of type \"" + cspec.getType()
	                                    + "\" from input does not match type "
	                                    + "\"" + rsmd.getColumnTypeName(i + 1)
	                                    + "\" in database at position " + i);
	                        }
	                        break;
	                    // check all double compatible types
	                    case Types.FLOAT:
	                    case Types.DOUBLE:
	                    case Types.NUMERIC:
	                    case Types.DECIMAL:
	                    case Types.REAL:
	                    case Types.BIGINT:
	                        // types must also be compatible to DoubleValue
	                        if (!cspec.getType().isCompatible(DoubleValue.class)) {
	                            throw new RuntimeException("Column \"" + name
	                                    + "\" of type \"" + cspec.getType()
	                                    + "\" from input does not match type "
	                                    + "\"" + rsmd.getColumnTypeName(i + 1)
	                                    + "\" in database at position " + i);
	                        }
	                        break;
	                    // check for data compatible types
	                    case Types.DATE:
	                    case Types.TIME:
	                    case Types.TIMESTAMP:
	                        // those types must also be compatible to DataValue
	                        if (!cspec.getType().isCompatible(
	                                DateAndTimeValue.class)) {
	                            throw new RuntimeException("Column \"" + name
	                                    + "\" of type \"" + cspec.getType()
	                                    + "\" from input does not match type "
	                                    + "\"" + rsmd.getColumnTypeName(i + 1)
	                                    + "\" in database at position " + i);
	                        }
	                        break;
	                    // all other cases are defined as StringValue types
	                }
	            }
	            rs.close();
	            statement.close();
	        } else {
	            mapping = new int[spec.getNumColumns()];
	            for (int k = 0; k < mapping.length; k++) {
	                mapping[k] = k;
	            }
	        }
	    } else {
        	LOGGER.info("writeData: not appendData");
        	
	        mapping = new int[spec.getNumColumns()];
	        for (int k = 0; k < mapping.length; k++) {
	            mapping[k] = k;
	        }
	        Statement statement = null;
	        try {
	            statement = conn.createStatement();
	            // remove existing table (if any)
	            final String query = "DROP TABLE " + table;
	            LOGGER.debug("Executing SQL statement \"" + query + "\"");
	            statement.execute(query);
	        } catch (Throwable t) {
	            if (statement == null) {
	                throw new SQLException("Could not create SQL statement, "
	                		+ "reason: " + t.getMessage(), t);
	            }
	            LOGGER.info("Can't drop table \"" + table
	                    + "\", will create new table.");
	        }
	        // and create new table
	        final String query = "CREATE TABLE " + table + " "
	            + createStmt(spec, sqlTypes)
                + " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
                + " LINES TERMINATED BY '\n'";
	        LOGGER.debug("Executing SQL statement \"" + query + "\"");
	        statement.execute(query);
	        statement.close();
	    }
	    
	    // INSERT‚Í"Load data into ..."‚É‚È‚éB
	    
	/*
	    // creates the wild card string based on the number of columns
	    // this string it used every time an new row is inserted into the db
	    final StringBuilder wildcard = new StringBuilder("(");
	    for (int i = 0; i < mapping.length; i++) {
	        if (i > 0) {
	            wildcard.append(", ?");
	        } else {
	            wildcard.append("?");
	        }
	    }
	    wildcard.append(")");
	
	    // problems writing more than 13 columns. the prepare statement
	    // ensures that we can set the columns directly row-by-row, the database
	    // will handle the commit
	    int rowCount = data.getRowCount();
	    int cnt = 1;
	    int errorCnt = 0;
	    int allErrors = 0;
	
	    // create table meta data with empty column information
	    final String query = "INSERT INTO "
	        + table + " VALUES " + wildcard.toString();
	    LOGGER.debug("Executing SQL statement \"" + query + "\"");
	    final PreparedStatement stmt = conn.prepareStatement(query);
	    try {
	        conn.setAutoCommit(false);
	        for (RowIterator it = data.iterator(); it.hasNext(); cnt++) {
	            exec.checkCanceled();
	            exec.setProgress(1.0 * cnt / rowCount, "Row " + "#" + cnt);
	            DataRow row = it.next();
	            for (int i = 0; i < mapping.length; i++) {
	                int dbIdx = i + 1;
	                if (mapping[i] < 0) {
	                    stmt.setNull(dbIdx, Types.NULL);
	                    continue;
	                }
	                DataColumnSpec cspec = spec.getColumnSpec(mapping[i]);
	                DataCell cell = row.getCell(mapping[i]);
	                if (cspec.getType().isCompatible(IntValue.class)) {
	                    if (cell.isMissing()) {
	                        stmt.setNull(dbIdx, Types.INTEGER);
	                    } else {
	                        int integer = ((IntValue) cell).getIntValue();
	                        stmt.setInt(dbIdx, integer);
	                    }
	                } else if (cspec.getType().isCompatible(
	                        DoubleValue.class)) {
	                    if (cell.isMissing()) {
	                        stmt.setNull(dbIdx, Types.DOUBLE);
	                    } else {
	                        double dbl = ((DoubleValue) cell).getDoubleValue();
	                        if (Double.isNaN(dbl)) {
	                            stmt.setNull(dbIdx, Types.DOUBLE);
	                        } else {
	                            stmt.setDouble(dbIdx, dbl);
	                        }
	                    }
	                } else if (cspec.getType().isCompatible(
	                        DateAndTimeValue.class)) {
	                    if (cell.isMissing()) {
	                        stmt.setNull(dbIdx, Types.DATE);
	                    } else {
	                        DateAndTimeValue dateCell = (DateAndTimeValue) cell;
	                    	if (!dateCell.hasTime() && !dateCell.hasMillis()) {
	                    		java.sql.Date date = new java.sql.Date(
	                    				dateCell.getUTCTimeInMillis());
	                    		stmt.setDate(dbIdx, date);
	                    	} else if (!dateCell.hasDate()) {
	                    		java.sql.Time time = new java.sql.Time(
	                    				dateCell.getUTCTimeInMillis());
	                    		stmt.setTime(dbIdx, time);
	                    	} else {
	                    		java.sql.Timestamp timestamp =
	                    		    new java.sql.Timestamp(
	                    		            dateCell.getUTCTimeInMillis());
	                    		stmt.setTimestamp(dbIdx, timestamp);
	                    	}
	                    }
	                } else {
	                    if (cell.isMissing()) {
	                        stmt.setNull(dbIdx, Types.VARCHAR);
	                    } else {
	                        stmt.setString(dbIdx, cell.toString());
	                    }
	                }
	            }
	            try {
	                stmt.execute();
	            } catch (Throwable t) {
	                allErrors++;
	                if (errorCnt > -1) {
	                    String errorMsg = "Error in row #" + cnt + ": "
	                        + row.getKey() + ", " + t.getMessage();
	                    exec.setMessage(errorMsg);
	                    if (errorCnt++ < 10) {
	                        LOGGER.warn(errorMsg);
	                    } else {
	                        errorCnt = -1;
	                        LOGGER.warn(errorMsg + " - more errors...", t);
	                    }
	                }
	            }
	        }
	        conn.commit();
	        conn.setAutoCommit(true);
	        if (allErrors == 0) {
	            return null;
	        } else {
	            return "Errors \"" + allErrors + "\" writing "
	                + rowCount + " rows.";
	        }
	    } finally {
	        stmt.close();
	    }
	    */
	    return null;
	}

	private static String replaceColumnName(final String oldName) {
	    return oldName.replaceAll("[^a-zA-Z0-9]", "_");
	}

	private static String createStmt(final DataTableSpec spec,
	        final Map<String, String> sqlTypes) {
	    StringBuilder buf = new StringBuilder("(");
	    for (int i = 0; i < spec.getNumColumns(); i++) {
	        if (i > 0) {
	            buf.append(", ");
	        }
	        DataColumnSpec cspec = spec.getColumnSpec(i);
	        String colName = cspec.getName();
	        String column = replaceColumnName(colName);
	        buf.append(column + " " + sqlTypes.get(colName));
	    }
	    buf.append(")");
	    return buf.toString();
	}

	/**
     * RowIterator via a database ResultSet.
     */
    private class DBRowIterator extends RowIterator {

        private final ResultSet m_result;

        private boolean m_hasExceptionReported = false;

        private int m_rowCounter = 0;

        /** FIXME: Some database (such as sqlite do NOT support) methods like
         * #getAsciiStream nor #getBinaryStream and will fail with an
         * SQLException. To prevent this exception for each ResultSet's value,
         * this flag for each column indicated that this exception has been
         * thrown and we directly can access the value via #getString.
         */
        private final boolean[] m_streamException;

        /**
         * Creates new iterator.
         * @param result result set to iterate
         */
        DBRowIterator(final ResultSet result) {
            m_result = result;
            m_streamException = new boolean[m_spec.getNumColumns()];
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            boolean ret = false;
            try {
                ret = m_result.next();
            } catch (SQLException sql) {
                ret = false;
            }
            if (!ret) {
                try {
                    m_result.close();
                } catch (SQLException e) {
                    LOGGER.error("SQL Exception while closing result set: ", e);
                }
            }
            return ret;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DataRow next() {
            DataCell[] cells = new DataCell[m_spec.getNumColumns()];
            for (int i = 0; i < cells.length; i++) {
                DataType type = m_spec.getColumnSpec(i).getType();
                int dbType = Types.NULL;
                final DataCell cell;
                try {
                    dbType = m_result.getMetaData().getColumnType(i + 1);
                    if (type.isCompatible(IntValue.class)) {
                        switch (dbType) {
                            // all types that can be interpreted as integer
                            case Types.TINYINT:
                                cell = readByte(i);
                                break;
                            case Types.SMALLINT:
                                cell = readShort(i);
                                break;
                            case Types.INTEGER:
                                cell = readInt(i);
                                break;
                            case Types.BIT:
                            case Types.BOOLEAN:
                                cell = readBoolean(i);
                                break;
                            default: cell = readInt(i);
                        }
                    } else if (type.isCompatible(DoubleValue.class)) {
                        switch (dbType) {
                            // all types that can be interpreted as double
                            case Types.REAL:
                                cell = readFloat(i);
                                break;
                            case Types.FLOAT:
                            case Types.DOUBLE:
                                cell = readDouble(i);
                                break;
                            case Types.DECIMAL:
                            case Types.NUMERIC:
                                cell = readBigDecimal(i);
                                break;
                            case Types.BIGINT:
                                cell = readLong(i);
                                break;
                            default: cell = readDouble(i);
                        }
                    } else if (type.isCompatible(DateAndTimeValue.class)) {
                        switch (dbType) {
                            case Types.DATE:
                                cell = readDate(i); break;
                            case Types.TIME:
                                cell = readTime(i); break;
                            case Types.TIMESTAMP:
                                cell = readTimestamp(i); break;
                            default: cell = readString(i);
                        }
                    } else {
                        switch (dbType) {
                            case Types.CLOB:
                                cell = readClob(i); break;
                            case Types.BLOB:
                                cell = readBlob(i); break;
                            case Types.ARRAY:
                                cell = readArray(i); break;
                            case Types.CHAR:
                            case Types.VARCHAR:
                                cell = readString(i); break;
                            case Types.LONGVARCHAR:
                                cell = readAsciiStream(i); break;
                            case Types.BINARY:
                            case Types.VARBINARY:
                                cell = readBytes(i); break;
                            case Types.LONGVARBINARY:
                                cell = readBinaryStream(i); break;
                            case Types.REF:
                                cell = readRef(i); break;
                            case Types.NCHAR:
                            case Types.NVARCHAR:
                            case Types.LONGNVARCHAR:
                                cell = readNString(i); break;
                            case Types.NCLOB:
                                cell = readNClob(i); break;
                            case Types.DATALINK:
                                cell = readURL(i); break;
                            case Types.STRUCT:
                            case Types.JAVA_OBJECT:
                                cell = readObject(i); break;
                            default:
                                cell = readObject(i); break;

                        }
                    }
                    // finally set the new cell into the array of cells
                    cells[i] = cell;
                } catch (SQLException sqle) {
                    handlerException(
                            "SQL Exception reading Object of type \""
                            + dbType + "\": ", sqle);
                } catch (IOException ioe) {
                    handlerException(
                            "I/O Exception reading Object of type \""
                            + dbType + "\": ", ioe);
                }
            }
            int rowId = m_rowCounter;
            try {
                rowId = m_result.getRow();
            } catch (SQLException sqle) {
                 // ignored
            }
            m_rowCounter++;
            return new DefaultRow(RowKey.createRowKey(rowId), cells);
        }

        private DataCell readClob(final int i)
                throws IOException, SQLException {
            Clob clob = m_result.getClob(i + 1);
            if (wasNull() || clob == null) {
                return DataType.getMissingCell();
            } else {
                Reader reader = clob.getCharacterStream();
                StringWriter writer = new StringWriter();
                FileUtil.copy(reader, writer);
                reader.close();
                writer.close();
                return new StringCell(writer.toString());
            }
        }

        private DataCell readNClob(final int i)
                throws IOException, SQLException {
            NClob nclob = m_result.getNClob(i + 1);
            if (wasNull() || nclob == null) {
                return DataType.getMissingCell();
            } else {
                Reader reader = nclob.getCharacterStream();
                StringWriter writer = new StringWriter();
                FileUtil.copy(reader, writer);
                reader.close();
                writer.close();
                return new StringCell(writer.toString());
            }
        }

        private DataCell readBlob(final int i)
                throws IOException, SQLException {
           Blob blob = m_result.getBlob(i + 1);
           if (wasNull() || blob == null) {
               return DataType.getMissingCell();
           } else {
               InputStreamReader reader =
                   // TODO: using default encoding
                   new InputStreamReader(blob.getBinaryStream());
               StringWriter writer = new StringWriter();
               FileUtil.copy(reader, writer);
               reader.close();
               writer.close();
               return new StringCell(writer.toString());
           }
        }

        private DataCell readAsciiStream(final int i)
                throws IOException, SQLException {
            if (m_streamException[i]) {
                return readString(i);
            }
            try {
                InputStream is = m_result.getAsciiStream(i + 1);
                if (wasNull() || is == null) {
                    return DataType.getMissingCell();
                } else {
                    InputStreamReader reader =
                    // TODO: using default encoding
                            new InputStreamReader(is);
                    StringWriter writer = new StringWriter();
                    FileUtil.copy(reader, writer);
                    reader.close();
                    writer.close();
                    return new StringCell(writer.toString());
                }
            } catch (SQLException sql) {
                m_streamException[i] = true;
                handlerException("Can't read from ASCII stream, "
                        + "trying to read string... ", sql);
                return readString(i);
            }
        }

        private DataCell readByte(final int i) throws SQLException {
            byte b = m_result.getByte(i + 1);
            if (wasNull()) {
                return DataType.getMissingCell();
            } else {
                return new IntCell(b);
            }
        }

        private DataCell readShort(final int i) throws SQLException {
            short s = m_result.getShort(i + 1);
            if (wasNull()) {
                return DataType.getMissingCell();
            } else {
                return new IntCell(s);
            }
        }

        private DataCell readInt(final int i) throws SQLException {
            int integer = m_result.getInt(i + 1);
            if (wasNull()) {
                return DataType.getMissingCell();
            } else {
                return new IntCell(integer);
            }
        }

        private DataCell readBoolean(final int i) throws SQLException {
            boolean b = m_result.getBoolean(i + 1);
            if (wasNull()) {
                return DataType.getMissingCell();
            } else {
                return new IntCell(b ? 1 : 0);
            }
        }

        private DataCell readDouble(final int i) throws SQLException {
            double d = m_result.getDouble(i + 1);
            if (wasNull()) {
                return DataType.getMissingCell();
            } else {
                return new DoubleCell(d);
            }
        }

        private DataCell readFloat(final int i) throws SQLException {
            float f = m_result.getFloat(i + 1);
            if (wasNull()) {
                return DataType.getMissingCell();
            } else {
                return new DoubleCell(f);
            }
        }

        private DataCell readLong(final int i) throws SQLException {
            long l = m_result.getLong(i + 1);
            if (wasNull()) {
                return DataType.getMissingCell();
            } else {
                return new DoubleCell(l);
            }
        }

        private DataCell readString(final int i) throws SQLException {
            String s = m_result.getString(i + 1);
            if (wasNull() || s == null) {
                return DataType.getMissingCell();
            } else {
                return new StringCell(s);
            }
        }

        private DataCell readBytes(final int i) throws SQLException {
            byte[] bytes = m_result.getBytes(i + 1);
            if (wasNull() || bytes == null) {
                return DataType.getMissingCell();
            } else {
                return new StringCell(new String(bytes));
            }
        }

        private DataCell readBigDecimal(final int i) throws SQLException {
            BigDecimal bc = m_result.getBigDecimal(i + 1);
            if (wasNull() || bc == null) {
                return DataType.getMissingCell();
            } else {
                return new DoubleCell(bc.doubleValue());
            }

        }

        private DataCell readBinaryStream(final int i)
                throws IOException, SQLException {
            if (m_streamException[i]) {
                return readString(i);
            }
            try {
                InputStream is = m_result.getBinaryStream(i + 1);
                if (wasNull() || is == null) {
                    return DataType.getMissingCell();
                } else {
                    InputStreamReader reader =
                    // TODO: using default encoding
                            new InputStreamReader(is);
                    StringWriter writer = new StringWriter();
                    FileUtil.copy(reader, writer);
                    reader.close();
                    writer.close();
                    return new StringCell(writer.toString());
                }
            } catch (SQLException sql) {
                m_streamException[i] = true;
                handlerException("Can't read from binary stream, "
                        + "trying to read string... ", sql);
                return readString(i);
            }
        }

        private DataCell readNString(final int i) throws SQLException {
            String str = m_result.getNString(i + 1);
            if (wasNull() || str == null) {
                return DataType.getMissingCell();
            } else {
                return new StringCell(str);
            }
        }

        private DataCell readDate(final int i) throws SQLException {
            Date date = m_result.getDate(i + 1);
            if (wasNull() || date == null) {
                return DataType.getMissingCell();
            } else {
                return new DateAndTimeCell(date.getTime(), true, false, false);
            }
        }

        private DataCell readTime(final int i) throws SQLException {
            Time time = m_result.getTime(i + 1);
            if (wasNull() || time == null) {
                return DataType.getMissingCell();
            } else {
                return new DateAndTimeCell(time.getTime(), false, true, true);
            }
        }

        private DataCell readTimestamp(final int i) throws SQLException {
            Timestamp timestamp = m_result.getTimestamp(i + 1);
            if (wasNull() || timestamp == null) {
                return DataType.getMissingCell();
            } else {
                return new DateAndTimeCell(
                        timestamp.getTime(), true, true, true);
            }
        }

        private DataCell readArray(final int i) throws SQLException {
            Array array = m_result.getArray(i + 1);
            if (wasNull() || array == null) {
                return DataType.getMissingCell();
            } else {
                return new StringCell(array.getArray().toString());
            }
        }

        private DataCell readRef(final int i) throws SQLException {
            Ref ref = m_result.getRef(i + 1);
            if (wasNull() || ref == null) {
                return DataType.getMissingCell();
            } else {
                return new StringCell(ref.getObject().toString());
            }
        }

        private DataCell readURL(final int i) throws SQLException {
            URL url = m_result.getURL(i + 1);
            if (url == null || wasNull()) {
                return DataType.getMissingCell();
            } else {
                return new StringCell(url.toString());
            }
        }

        private DataCell readObject(final int i) throws SQLException {
            Object o = m_result.getObject(i + 1);
            if (o == null || wasNull()) {
                return DataType.getMissingCell();
            } else {
                return new StringCell(o.toString());
            }
        }

        private boolean wasNull() {
            try {
                return m_result.wasNull();
            } catch (SQLException sqle) {
                handlerException("SQL Exception: ", sqle);
                return true;
            }
        }

        private void handlerException(final String msg, final Exception e) {
            if (m_hasExceptionReported) {
                LOGGER.debug(msg + e.getMessage(), e);
            } else {
                m_hasExceptionReported = true;
                LOGGER.error(msg + e.getMessage()
                        + " - all further errors are suppressed "
                        + "and reported on debug level only", e);
            }
        }
    }
}