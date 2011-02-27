package jp.co.recruit.hadoop.knime.writer;

import java.awt.BorderLayout;

import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.JEditorPane;
import javax.swing.JList;
import javax.swing.JPanel;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

/**
 *
 * @author Thomas Gabriel, University of Konstanz
 */
class FileTransferClientDialogPane extends NodeDialogPane {

    private final HiveDialogPane m_HiveDialogPane = new HiveDialogPane();

    // private final DefaultListModel m_listModelVars;

    private final AdditionalPanel m_advancedPanel;

    /**
     * Creates new dialog.
     * @param hasLoginPane true, if a login pane is visible, otherwise false
     */
    FileTransferClientDialogPane(final boolean hasLoginPane) {
        super();
                
        JPanel allPanel = new JPanel(new BorderLayout());

        allPanel.add(m_HiveDialogPane, BorderLayout.NORTH);

        // init variable list
        // m_listModelVars = new DefaultListModel();

        super.addTab("Settings", allPanel);
        
        m_advancedPanel = new AdditionalPanel();
        super.addTab("Advanced", m_advancedPanel);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
            final PortObjectSpec[] specs) throws NotConfigurableException {

    	try {
        	m_HiveDialogPane.loadSettingsFrom(settings, specs);
            m_advancedPanel.loadValuesIntoPanel(settings);
		} catch (InvalidSettingsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings)
            throws InvalidSettingsException {
    	
		m_HiveDialogPane.saveSettingsTo(settings);
        m_advancedPanel.saveValuesFromPanelInto(settings);
    }
}
