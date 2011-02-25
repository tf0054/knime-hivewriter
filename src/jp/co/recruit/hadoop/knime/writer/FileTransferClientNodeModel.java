package jp.co.recruit.hadoop.knime.writer;


import org.knime.base.node.io.csvwriter.CSVWriter;
import org.knime.base.node.io.csvwriter.FileWriterSettings;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.RowKey;
import org.knime.core.data.date.DateAndTimeValue;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;

import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import jp.co.recruit.hadoop.knime.writer.FileWriterNodeSettings.FileOverwritePolicy;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs.VFS;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemManager;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.provider.sftp.SftpFileSystemConfigBuilder;



/**
 *
 * @author Thomas Gabriel, University of Konstanz
 */
final class FileTransferClientNodeModel extends NodeModel {

	DatabaseQueryConnectionSettings conn = null;
	
    private FileTransferClientConfig m_config;
    private FileWriterNodeSettings m_settings;

    private final Map<String, String> m_types =
        new LinkedHashMap<String, String>();

    public Set<String> tables = new HashSet<String>();

	private static final NodeLogger LOGGER =
        NodeLogger.getLogger(FileTransferClientNodeModel.class);

    /** Default SQL-type for Strings. */
    static final String SQL_TYPE_STRING = "STRING";

    /** Default SQL-type for Integers. */
    static final String SQL_TYPE_INTEGER = "INT";

    /** Default SQL-type for Doubles. */
    static final String SQL_TYPE_DOUBLE = "DOUBLE";

    /** Default SQL-type for Date. */
    static final String SQL_TYPE_DATEANDTIME = "STRING";

    /** Config key for column to SQL-type mapping. */
    static final String CFG_SQL_TYPES = "sql_types";

    /**
     * Creates a new database reader.
     */
    
    FileTransferClientNodeModel(final int ins, final int outs) {
        super(ins, outs);
        // CSV書き出し系の設定はダイアログ変更できない(させない)
        // つまりXML書き出しとかもされない想定(Loaderと同じくm_configが書きだされる)
        m_settings = new FileWriterNodeSettings();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
    	LOGGER.info("saveSettingsTo");
        if (m_config != null) {
        	m_config.tables = new HashSet<String>(tables);
        	LOGGER.info("tables: " + m_config.tables);
            m_config.saveSettingsTo(settings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {

    	// new FileTransferClientConfig().loadSettingsInModel(settings);
    	
    	// driver,database,user,passwordは予約語ぽい
    	// これらはpackage org.knime.core.node.port.database.DatabaseConnectionSettingsにあり
        if (conn == null) {
        	LOGGER.info("validSettings: conn is created.");
        	conn = new DatabaseQueryConnectionSettings(settings, getCredentialsProvider());
        }else{
        	LOGGER.info("validSettings: conn is OK.");
        }
    	ResultSet result = execHiveQuery("show tables");
    	if(result == null){
        	LOGGER.error("tab_name: (There are no tables)");
    	}else{
			tables.clear();
	    	try {
		        while ( result.next() ) {
			    	// "show tables"の結果表示
		        	LOGGER.info("tab_name: " + result.getString("tab_name"));
		        	tables.add(result.getString("tab_name"));
		        }    	
			} catch (SQLException e) {
				e.printStackTrace();
			}
    	}
    }

    protected ResultSet execHiveQuery(String strQuery){
    	ResultSet result = null;
        try {
            if (conn == null) {
                throw new InvalidSettingsException(
                        "No database connection available.");
            }else{
            	LOGGER.info("execHiveQuery: conn may be OK");
            }
            final Connection connObj = conn.createConnection();
            final Statement m_stmt = connObj.createStatement();
            result = m_stmt.executeQuery(strQuery);
        	LOGGER.info("execHiveQuery: " + strQuery);
        } catch (Exception e) {
            e.printStackTrace();
        }
    	return result;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {

    	FileTransferClientConfig config = new FileTransferClientConfig();
        config.loadSettingsInModel(settings);
        m_config = config;                
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
            final ExecutionContext exec) throws CanceledExecutionException,
            IOException {

    	int intRet = -2;
        final String strFileName = "/tmp/hivewriter_" + System.identityHashCode(this) + ".csv";
    	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    	// 結果をファイルに書き出す
    	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    	m_settings = new FileWriterNodeSettings();
    	csv_execute(inData,exec);
    	
    	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    	// 当該ファイルをsftpで送り込む
    	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        exec.setProgress("Sending file ...");
    	FileSystemManager fsm = VFS.getManager();
    	FileSystemOptions opts = new FileSystemOptions();
    	
    	// known_hostsのチェックをスキップするよう設定します
    	SftpFileSystemConfigBuilder builder = SftpFileSystemConfigBuilder.getInstance();
    	builder.setStrictHostKeyChecking(opts, "no");
    	
    	// sftpプロトコルを指定してファイルを取得します
    	// （ホームディレクトリからの相対パス指定はうまく動作しませんでした）
    	String execUrl = "sftp://"
			+ m_config.m_user + ":" + "xxx" + "@"
			+ m_config.m_serverurl + strFileName;    	
    	LOGGER.info("execUrl: " + execUrl);
    	execUrl = "sftp://"
			+ m_config.m_user + ":" + m_config.m_password + "@"
			+ m_config.m_serverurl + strFileName;    	
    	FileObject fileObj = fsm.resolveFile(execUrl, opts);
    	
    	// IOUtilsの使い方
    	// String fileStr = IOUtils.toString(file.getContent().getInputStream(), "UTF-8");
        // IOUtils.write(HELLO_HADOOP_STR, output);

    	InputStream input = new FileInputStream(m_settings.getFileName());
        OutputStream output = fileObj.getContent().getOutputStream();
        Integer length = copyStream(input,output,2048);
        if(length > 0){
        	intRet++;
        	LOGGER.info("transferedSize: " + length.toString());
        }else{
        	LOGGER.error("transfer was failed.");
        }
        IOUtils.closeQuietly(output);

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    	// TBLの存在をチェックし、存在しなければ作る
    	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
        try {
        	LOGGER.info("execute: writeData");
            HiveConnectionRW objHiveConn = new HiveConnectionRW(conn);
            
			String error = objHiveConn.writeData(
					m_config.table, inData[0], false, exec, m_types);
        	LOGGER.info("execute: writeData: "+error);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    	// JDBC経由で"LOAD DATA LOCAL INPATH './examples/files/kv1.txt' OVERWRITE INTO TABLE pokes"
    	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
        exec.setProgress("Making Hive load file ...");
        ResultSet result = execHiveQuery(
        		"load data local inpath '" + strFileName + "' overwrite into table " + m_config.table
        );
    	if(result != null){
        	intRet++;
            LOGGER.info("hive load may be OK.");
    	}else{
            LOGGER.error("cannot load data on hive.");
    	}
        
    	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    	// 次ノードに引き継ぐテーブルをゼロから作る
    	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    	// init spec
        exec.setProgress("Making result table ...");
		BufferedDataContainer container = exec.createDataContainer(makeTableSpec());

		// add arbitrary number of rows to the container
		DataRow firstRow = new DefaultRow(
			new RowKey("first"), // KNIME専用のデータ
			new DataCell[]{new StringCell("result"), new IntCell(intRet)} ); // 実データ
		container.addRowToTable(firstRow);
    	
		/*
    	DataRow secondRow = new DefaultRow(new RowKey("second"), new DataCell[]{
    	    new StringCell("B1"), new DoubleCell(2.0)
    	});
    	container.addRowToTable(secondRow);
    	*/

    	// finally close the container and get the result table.
    	container.close();
    	  
        return new BufferedDataTable[0];
    }
    
    protected DataTableSpec makeTableSpec(){
    	DataTableSpec spec = new DataTableSpec(
    			new DataColumnSpecCreator("A", StringCell.TYPE).createSpec(),
    			new DataColumnSpecCreator("B", IntCell.TYPE).createSpec() );
    	return spec;
    }

    // CSVファイルに、前から来たBufferTableを出力する
    protected void csv_execute(final BufferedDataTable[] data,
            final ExecutionContext exec) throws CanceledExecutionException,
            IOException {
        DataTable in = data[0];

        File file = new File(m_settings.getFileName());
        File parentDir = file.getParentFile();
        boolean dirCreated = false;
        if (!parentDir.exists()) {
            if (!parentDir.mkdirs()) {
                throw new IllegalStateException("Unable to create directory"
                        + " for specified output file: "
                        + parentDir.getAbsolutePath());
            }
            LOGGER.info("Created directory for specified output file: "
                    + parentDir.getAbsolutePath());
            dirCreated = true;
        }

        // figure out if the writer is actually supposed to write col headers
        boolean writeColHeader = m_settings.writeColumnHeader();
        if (writeColHeader && file.exists()) {
            if (m_settings.getFileOverwritePolicy().equals(
                    FileOverwritePolicy.Append)) {
                // do not write headers if the file exists and we append to it
                writeColHeader = !m_settings.skipColHeaderIfFileExists();
            }
        }
        // make a copy of the settings with the modified value
        FileWriterSettings writerSettings = new FileWriterSettings(m_settings);
        writerSettings.setWriteColumnHeader(writeColHeader);

        boolean appendToFile;
        if (file.exists()) {
            switch (m_settings.getFileOverwritePolicy()) {
            case Append:
                appendToFile = true;
                break;
            case Abort:
                throw new RuntimeException("File \"" + file.getAbsolutePath()
                        + "\" exists, must not overwrite it (check "
                        + "dialog settings)");
            case Overwrite:
                appendToFile = false;
                break;
            default:
                throw new InternalError("Unknown case: "
                        + m_settings.getFileOverwritePolicy());
            }
        } else {
            appendToFile = false;
        }
        CSVWriter tableWriter =
                new CSVWriter(new FileWriter(file, appendToFile),
                        writerSettings);

        // write the comment header, if we are supposed to
        writeCommentHeader(m_settings, tableWriter, data[0], appendToFile);

        try {
            tableWriter.write(in, exec);
        } catch (CanceledExecutionException cee) {
            LOGGER.info("Table FileWriter canceled.");
            if (dirCreated) {
                LOGGER.warn("The directory for the output file was created and"
                        + " is not removed.");
            }
            tableWriter.close();
            if (file.delete()) {
                LOGGER.debug("File " + m_settings.getFileName() + " deleted.");
                if (dirCreated) {
                    LOGGER.debug("The directory created for the output file"
                            + " is not removed though.");
                }
            } else {
                LOGGER.warn("Unable to delete file '"
                        + m_settings.getFileName() + "' after cancellation.");
            }
            throw cee;
        }

        tableWriter.close();

        if (tableWriter.hasWarningMessage()) {
            setWarningMessage(tableWriter.getLastWarningMessage());
        }        
    }

    protected static int copyStream(InputStream in, OutputStream os,
	    int bufferSize) throws IOException {
	    int len = -1;
	    int length = 0;
	    byte[] b = new byte[bufferSize * 1024];
	    try {
	        while ((len = in.read(b, 0, b.length)) != -1) {
	            os.write(b, 0, len);
	            length += len;
	        }
	        os.flush();
	    } finally {
	        if (in != null) {
	            try {
	                in.close();
	            } catch (IOException e) {
	                e.printStackTrace();
	            }
	        }
	        if (os != null) {
	            try {
	                os.close();
	            } catch (IOException e) {
	                e.printStackTrace();
	            }
	        }
	    }
	    return length;
	}
    private void writeCommentHeader(final FileWriterNodeSettings settings,
            final BufferedWriter file, final DataTable inData,
            final boolean append) throws IOException {
        if ((file == null) || (settings == null)) {
            return;
        }
        if (isEmpty(settings.getCommentBegin())) {
            return;
        }

        // figure out if we have to write anything at all:
        boolean writeComment = false;
        writeComment |= settings.addCreationTime();
        writeComment |= settings.addCreationUser();
        writeComment |= settings.addTableName();
        writeComment |= notEmpty(settings.getCustomCommentLine());

        if (!writeComment) {
            return;
        }

        // if we have block comment patterns we write them only once. Otherwise
        // we add the commentBegin to every line.
        boolean blockComment = notEmpty(settings.getCommentEnd());

        if (blockComment) {
            file.write(settings.getCommentBegin());
            file.newLine();
        }

        // add date/time and user, if we are supposed to
        if (settings.addCreationTime() || settings.addCreationUser()) {
            if (!blockComment) {
                file.write(settings.getCommentBegin());
            }
            if (append) {
                file.write("   The following data was added ");
            } else {
                file.write("   This file was created ");
            }
            if (settings.addCreationTime()) {
                file.write("on " + new Date() + " ");
            }
            if (settings.addCreationUser()) {
                file.write("by user '" + System.getProperty("user.name") + "'");
            }
            file.newLine();
        }

        // add the table name
        if (settings.addTableName()) {
            if (!blockComment) {
                file.write(settings.getCommentBegin());
            }
            file.write("   The data was read from the \""
                    + inData.getDataTableSpec().getName() + "\" data table.");
            file.newLine();
        }

        // at last: add the user comment line
        if (notEmpty(settings.getCustomCommentLine())) {
            String[] lines = settings.getCustomCommentLine().split("\n");
            for (String line : lines) {
                if (!blockComment) {
                    file.write(settings.getCommentBegin());
                }
                file.write("   " + line);
                file.newLine();
            }
        }

        // close the block comment
        if (blockComment) {
            file.write(settings.getCommentEnd());
            file.newLine();
        }

    }

    static boolean isEmpty(final String s) {
        return !notEmpty(s);
    }
    
    static boolean notEmpty(final String s) {
        if (s == null) {
            return false;
        }
        return (s.length() > 0);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs){
    	LOGGER.info("configure: Model");
    	
        Map<String, String> map = new LinkedHashMap<String, String>();
        // check that each column has a assigned type
        for (int i = 0; i < inSpecs[0].getNumColumns(); i++) {
            final String name = inSpecs[0].getColumnSpec(i).getName();
            String sqlType = m_types.get(name);
            if (sqlType == null) {
                final DataType type = inSpecs[0].getColumnSpec(i).getType();
                if (type.isCompatible(IntValue.class)) {
                    sqlType = SQL_TYPE_INTEGER;
                } else if (type.isCompatible(DoubleValue.class)) {
                    sqlType = SQL_TYPE_DOUBLE;
                } else if (type.isCompatible(DateAndTimeValue.class)) {
                    sqlType = SQL_TYPE_DATEANDTIME;
                } else {
                    sqlType = SQL_TYPE_STRING;
                }
            }
            map.put(name, sqlType);
        }
        m_types.clear();
        m_types.putAll(map);
        
        return new DataTableSpec[0];
    }
    
    @Override
    protected void reset() {
    	// for changing jdbc settings.
        if (conn != null) {
        	conn = null;
        }
    	LOGGER.info("reset");
    }
    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir,
            final ExecutionMonitor exec)
            throws IOException {
        // Node has no internal data.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir,
            final ExecutionMonitor exec)
            throws IOException {
        // Node has no internal data.
    }

    /*    
    @Override
    protected void loadInternals(final File nodeInternDir,
            final ExecutionMonitor exec) throws IOException {
    	LOGGER.error("loadInternals");
        File specFile = null;
        specFile = new File(nodeInternDir, "spec.xml");
        if (!specFile.exists()) {
            IOException ioe = new IOException("Spec file (\""
                    + specFile.getAbsolutePath() + "\") does not exist "
                    + "(node may have been saved by an older version!)");
            throw ioe;
        }
        NodeSettingsRO specSett =
            NodeSettings.loadFromXML(new FileInputStream(specFile));
    }
 	*/
    /**
     * {@inheritDoc}
    @Override
    protected void saveInternals(final File nodeInternDir,
            final ExecutionMonitor exec) throws IOException {
    	LOGGER.error("saveInternals");
        
        NodeSettings specSett = new NodeSettings("spec.xml");
        // m_lastSpec.save(specSett);
        File specFile = new File(nodeInternDir, "spec.xml");
        specSett.saveToXML(new FileOutputStream(specFile));
    }
     */
    
    final DatabaseQueryConnectionSettings createDBQueryConnection(
            final DatabasePortObjectSpec spec, final String newQuery)
    		throws InvalidSettingsException {
    	DatabaseQueryConnectionSettings conn =
    		new DatabaseQueryConnectionSettings(
    			spec.getConnectionModel(), getCredentialsProvider());
        return new DatabaseQueryConnectionSettings(conn, newQuery);
    }
}