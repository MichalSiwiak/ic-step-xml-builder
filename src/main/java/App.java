import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class App {

    public static int count = 0;
    static Connection connection = getConnection();

    public static void main(String[] argv) throws SQLException {

        ArrayList<String> towKods = new ArrayList<>();

        String csvFile = "E:\\Downloads\\exported.csv";
        String line = "";

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            while ((line = br.readLine()) != null) {
                towKods.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        createSTEPXML(towKods);
        connection.close();
    }

    public static void createSTEPXML(ArrayList<String> towKods) {

        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

            // root elements
            Document doc = docBuilder.newDocument();
            Element rootElement = doc.createElement("STEP-ProductInformation");
            // ContextID
            Attr contextID = doc.createAttribute("ContextID");
            contextID.setValue("Global");
            rootElement.setAttributeNode(contextID);
            // WorkspaceID
            Attr workspaceID = doc.createAttribute("WorkspaceID");
            workspaceID.setValue("Main");
            rootElement.setAttributeNode(workspaceID);

            doc.appendChild(rootElement);

            // Products elements
            Element products = doc.createElement("Products");
            rootElement.appendChild(products);

            for (String towKod : towKods) {

                Map<String, StringBuilder> results = App.getTowCodesFromOracle(towKod, connection);
                System.out.println(count++ + " " + towKod + ": " + results.toString());

                if (results.size() != 0) {

                    // Products elements
                    Element product = doc.createElement("Product");
                    // ContextID
                    Attr productID = doc.createAttribute("ID");
                    productID.setValue("IC_SalesItem_" + towKod);
                    product.setAttributeNode(productID);
                    products.appendChild(product);
                    // Values elements
                    Element values = doc.createElement("Values");
                    product.appendChild(values);
                    // MultiValue elements
                    Element multiValue = doc.createElement("MultiValue");
                    values.appendChild(multiValue);
                    Attr attributeID = doc.createAttribute("AttributeID");
                    attributeID.setValue("IC_ATTR_TechnicalAttributesCheck");
                    multiValue.setAttributeNode(attributeID);
                    // Value elements

                    for (Map.Entry<String, StringBuilder> entry : results.entrySet()) {
                        Element value = doc.createElement("Value");
                        multiValue.appendChild(value);
                        value.appendChild(doc.createTextNode(entry.getKey() + "," + entry.getValue()));
                    }
                }

                // maksymalna ilość kursorów
                if (count % 1000 == 0) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } finally {
                        connection = getConnection();
                    }
                }
            }

            // write the content into xml file
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(new File("E:\\Downloads\\importTechnicalAttributes.xml"));

            // Output to console for testing
            // StreamResult result = new StreamResult(System.out);

            transformer.transform(source, result);

            System.out.println("File saved!");

        } catch (ParserConfigurationException pce) {
            pce.printStackTrace();
        } catch (TransformerException tfe) {
            tfe.printStackTrace();
        }
    }

    public static Connection getConnection() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            System.out.println("Where is your Oracle JDBC Driver?");
            e.printStackTrace();
            return null;
        }
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("url", "user", "password");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static Map<String, StringBuilder> getTowCodesFromOracle(String towKod, Connection connection) {

        Statement statement = null;
        ResultSet resultSet = null;
        Map<String, StringBuilder> map = new HashMap<>();

        try {

            statement = connection.createStatement();
            resultSet = statement.executeQuery(queryGetTechnicalAttributesOneTowKod(towKod));
            String typValue = null;

            while (resultSet.next()) {
                String nazwa_kryterium_pl = resultSet.getString("NAZWA_KRYTERIUM_PL");
                String typFlag = resultSet.getString("TYP");
                String isMultiValue = resultSet.getString("MULTI");

                switch (typFlag) {
                case "N":
                    typValue = resultSet.getString("TYPE_NUMERICAL");
                    break;
                case "T":
                    typValue = resultSet.getString("TYPE_TEXT_PL");
                    break;
                case "C":
                    typValue = resultSet.getString("TYPE_VARCHAR");
                    break;
                case "M":
                    typValue = resultSet.getString("TYPE_VARCHAR");
                    break;
                case "B":
                    typValue = resultSet.getString("TYPE_BOOL");
                    if (typValue != null) {
                        if (typValue.equals("N")) {
                            typValue = "Nie";
                        }
                        if (typValue.equals("T")) {
                            typValue = "Tak";
                        }
                    }
                    break;
                default:

                    File file = new File("E:\\Downloads\\log.txt");
                    FileWriter fileWriter = null;
                    BufferedWriter bufferedWriter = null;

                    try {
                        fileWriter = new FileWriter(file, true);
                        bufferedWriter = new BufferedWriter(fileWriter);
                        bufferedWriter.write("Nie znaleziono typu: " + "typ " + typFlag + "towKod " + towKod);
                        bufferedWriter.newLine();
                    } catch (IOException e) {
                    } finally {
                        try {
                            bufferedWriter.close();
                            fileWriter.close();
                        } catch (IOException e) {
                        }
                    }
                }

                if (map.containsKey(isMultiValue + " " + nazwa_kryterium_pl)) {
                    StringBuilder stringBuilder = map.get(isMultiValue + " " + nazwa_kryterium_pl);
                    stringBuilder.append("," + typValue);
                    map.put(isMultiValue + " " + nazwa_kryterium_pl, stringBuilder);
                } else {
                    map.put(isMultiValue + " " + nazwa_kryterium_pl, new StringBuilder().append(typValue));
                }
            }

        } catch (SQLException e) {

            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return null;
        }
        return map;
    }

    public static String queryGetTechnicalAttributesOneTowKod(String towKod) {
        return "SELECT\n" + "  tow_ic.towinf.tow_kod                   AS TOW_KOD,\n"
                + "  tow_ic.get_nazwa(TOW_IC.KRY.NAZ, 'PL')  AS NAZWA_KRYTERIUM_PL,\n"
                + "  tow_ic.get_nazwa(TOW_IC.KRY.NAZ, 'EN')  AS NAZWA_KRYTERIUM_EN,\n" + "  tow_ic.towinf.kry,\n"
                + "  TOW_IC.INFB.WAR                         AS TYPE_BOOL,\n"
                + "  TOW_IC.INFC.OPIS                        AS TYPE_VARCHAR,\n"
                + "  TOW_IC.INFD.OPIS                        AS TYPE_DATE,\n"
                + "  TOW_IC.INFN.OPIS                        AS TYPE_NUMERICAL,\n"
                + "  tow_ic.get_nazwa(TOW_IC.INFT.NAZ, 'PL') AS TYPE_TEXT_PL,\n"
                + "  tow_ic.get_nazwa(TOW_IC.INFT.NAZ, 'EN') AS TYPE_TEXT_EN,\n" + "  TOW_IC.KRY.TYP,\n"
                + "  TOW_IC.KRY.MULTI,\n" + "  TOW_IC.KRY.MASKA,\n" + "  TOW_IC.KRY.MULTI_KOL,\n"
                + "  TOW_IC.KRY.MULTI_SEP\n" + "FROM TOW_IC.TOWINF\n"
                + "  LEFT JOIN TOW_IC.KRY ON TOW_IC.KRY.KRY = TOW_IC.TOWINF.KRY\n" + "\n"
                + "  LEFT JOIN TOW_IC.INFB ON TOW_IC.TOWINF.INF = TOW_IC.INFB.INF\n"
                + "  LEFT JOIN TOW_IC.INFC ON TOW_IC.TOWINF.INF = TOW_IC.INFC.INF\n"
                + "  LEFT JOIN TOW_IC.INFD ON TOW_IC.TOWINF.INF = TOW_IC.INFD.INF\n"
                + "  LEFT JOIN TOW_IC.INFN ON TOW_IC.TOWINF.INF = TOW_IC.INFN.INF\n"
                + "  LEFT JOIN TOW_IC.INFT ON TOW_IC.TOWINF.INF = TOW_IC.INFT.INF\n" + "WHERE TOW_IC.TOWINF.TOW_KOD ='"
                + towKod + "'";
    }
}