package jp.co.recruit.hadoop.knime.writer;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 *
 */
class AdditionalPanel extends JPanel {

    private static final Dimension TEXTFIELDDIM = new Dimension(75, 25);

    private JTextField m_colSeparator = new JTextField("");

    private JTextField m_missValuePattern = new JTextField("");

    private final JCheckBox m_csvCheckBox = new JCheckBox();
    static final Font FONT = new Font("Monospaced", Font.PLAIN, 12);

    /**
     * 
     */
    public AdditionalPanel() {

        JPanel credPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        credPanel.setBorder(BorderFactory.createTitledBorder(
            " Use csv-serde "));
        credPanel.add(m_csvCheckBox);
        // m_csvCheckBox.setEditable(false);
        m_csvCheckBox.setFont(FONT);
        m_csvCheckBox.setPreferredSize(new Dimension(375, 20));
        add(credPanel);
        
        /*
        JPanel missPanel = new JPanel();
        missPanel.setLayout(new BoxLayout(missPanel, BoxLayout.X_AXIS));
        missPanel.setBorder(BorderFactory.createTitledBorder(BorderFactory
                .createEtchedBorder(), "Missing Value Pattern"));
        missPanel.add(new JLabel("Pattern written out for missing values:"));
        missPanel.add(Box.createHorizontalStrut(5));
        missPanel.add(m_missValuePattern);
        m_missValuePattern.setPreferredSize(TEXTFIELDDIM);
        m_missValuePattern.setMaximumSize(TEXTFIELDDIM);
        missPanel.add(Box.createHorizontalGlue());

        JPanel colSepPanel = new JPanel();
        colSepPanel.setLayout(new BoxLayout(colSepPanel, BoxLayout.X_AXIS));
        colSepPanel.setBorder(BorderFactory.createTitledBorder(BorderFactory
                .createEtchedBorder(), "Data Separator"));
        colSepPanel.add(new JLabel("Pattern written out between data values:"));
        colSepPanel.add(Box.createHorizontalStrut(5));
        colSepPanel.add(m_colSeparator);
        m_colSeparator.setPreferredSize(TEXTFIELDDIM);
        m_colSeparator.setMaximumSize(TEXTFIELDDIM);
        m_colSeparator.setToolTipText("Use \\t or \\n "
                + "for a tab or newline character.");
        colSepPanel.add(Box.createHorizontalGlue());

        setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
        add(Box.createVerticalGlue());
        add(colSepPanel);
        add(Box.createVerticalStrut(10));
        add(missPanel);
        add(Box.createVerticalGlue());
        add(Box.createVerticalGlue());
		*/
    }

    /**
     * Reads new values from the specified object and puts them into the panel's
     * components.
     * 
     * @param settings object holding the new values to show.
     */
    void loadValuesIntoPanel(final NodeSettingsRO settings) {

    	try {
			if(settings.getBoolean("csvSerde")){
				m_csvCheckBox.setSelected(true);
			}else{
				m_csvCheckBox.setSelected(false);
			}
		} catch (InvalidSettingsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
        /*
        String colSep = settings.getColSeparator();
        colSep = FileWriterSettings.escapeString(colSep);

        m_colSeparator.setText(colSep);
        m_missValuePattern.setText(settings.getMissValuePattern());
        */

    }

    /**
     * Writes the current values from the components into the settings object.
     * 
     * @param settings the object to write the values into
     */
    void saveValuesFromPanelInto(final NodeSettingsWO settings) {

    	settings.addBoolean("csvSerde", m_csvCheckBox.isSelected());
    	
        /*
        String colSep = m_colSeparator.getText();
        colSep = FileWriterSettings.unescapeString(colSep);

        settings.setColSeparator(colSep);
        settings.setMissValuePattern(m_missValuePattern.getText());
        */
    }

}
