package br.ufes.inf.nemo.integradoce.etl;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.PrefixManager;
import org.semanticweb.owlapi.util.DefaultPrefixManager;

public class UnespExtractTransform {

    // following examples in
    // https://github.com/owlcs/owlapi/blob/version4/contract/src/test/java/org/semanticweb/owlapi/examples/Examples.java

    private static OWLOntologyManager manager;
    private static OWLDataFactory dataFactory;

    /**
     * Prefixes are required for all operations that will obtain instances of
     * OWLClass, OWLNamedIndividual
     */
    private static PrefixManager rdfspm, gufopm, docepm, integradocepm;

    /**
     * Represents the Doce River
     */
    private static OWLNamedIndividual riodoce;

    private final static Logger LOGGER = Logger.getLogger("ETL");

    static {
        manager = OWLManager.createOWLOntologyManager();
        dataFactory = manager.getOWLDataFactory();
        rdfspm = new DefaultPrefixManager(null, null, "http://www.w3.org/2000/01/rdf-schema#");
        gufopm = new DefaultPrefixManager(null, null, "http://purl.org/nemo/gufo#");
        docepm = new DefaultPrefixManager(null, null, "http://purl.org/nemo/doce#");
        integradocepm = new DefaultPrefixManager(null, null, "http://purl.org/nemo/integradoce#");
        riodoce = dataFactory.getOWLNamedIndividual(":RioDoce", docepm);
    }

    public static void extractTransformGeographicPointsUNESP(String csvFilePath, OWLOntology ontology)
            throws ParseException, IOException {

        // TODO add geographical information such as city, municipality, river basin

        Reader in = new FileReader(csvFilePath);

        // header:
        // ID amostras;Referência;Coordenada Geográfica (UTM);;Data
        // Example row:
        // M2;Rio Gualaxo do Norte em Bento Rodrigues;-20,27638884;-43,43115158;30/04/16

        Iterable<CSVRecord> records = CSVFormat.EXCEL.withDelimiter(';').withFirstRecordAsHeader().parse(in);
        for (CSVRecord record : records) {
            String codigo = record.get(0);
            String nome = record.get(1);
            String descricao = record.get(1);
            String latitude = record.get(2);
            String longitude = record.get(3);

            NumberFormat nf = NumberFormat.getInstance(Locale.forLanguageTag("pt-BR"));
            Load.addGeographicPoint(ontology, ":UNESP_" + codigo.replaceAll("\\s", "-"),
                    nf.parse(latitude).floatValue(),
                    nf.parse(longitude).floatValue(), descricao, nome);
        }
    }

    public static void extractTransformUNESP(File file, OWLOntology ontology) throws IOException, ParseException {

        LOGGER.info("Processing " + file.getName());

        // campanhas 1 e 2
        // As Cd Co Cr Mn Ni Pb Fe-diss Fe-tot Al-diss Al-tot
        // campanhas 4 e 5
        // As Cd Cr Cu Mn Ni Pb Zn Fe Al
        HashMap<String, String> qualityKindRecord = new HashMap<String, String>() {
            {
                put("As", "http://purl.org/nemo/doce#TotalArsenicConcentration");
                put("Cd", "http://purl.org/nemo/doce#TotalCadmiumConcentration");
                put("Co", "http://purl.org/nemo/doce#TotalCobaltConcentration");
                put("Cu", "http://purl.org/nemo/doce#TotalCopperConcentration");
                put("Cr", "http://purl.org/nemo/doce#TotalChromiumConcentration");
                put("Mn", "http://purl.org/nemo/doce#TotalManganeseConcentration");
                put("Ni", "http://purl.org/nemo/doce#TotalNickelConcentration");
                put("Pb", "http://purl.org/nemo/doce#TotalLeadConcentration");
                put("Zn", "http://purl.org/nemo/doce#TotalZincConcentration");
                put("Fe-diss", "http://purl.org/nemo/doce#DissolvedIronConcentration");
                put("Fe-tot", "http://purl.org/nemo/doce#TotalIronConcentration");
                put("Fe", "http://purl.org/nemo/doce#TotalIronConcentration");
                put("Al-diss", "http://purl.org/nemo/doce#DissolvedAluminiumConcentration");
                put("Al-tot", "http://purl.org/nemo/doce#TotalAluminiumConcentration");
                put("Al", "http://purl.org/nemo/doce#TotalAluminiumConcentration");
            }
        };

        Reader inHeaderMapping = new FileReader(file);
        CSVParser parser = CSVFormat.EXCEL.withDelimiter(';').withFirstRecordAsHeader().parse(inHeaderMapping);
        List<String> headers = parser.getHeaderNames();

        Reader in = new FileReader(file);
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withDelimiter(';').withFirstRecordAsHeader().parse(in);
        for (CSVRecord record : records) {
            for (String header : headers) {
                if (header.contains("Amostra") || header.contains("Data"))
                    continue;
                if (record.get(header).equals("-"))
                    continue;
                String codigo = record.get(1);

                String data = record.get(0);
                String hora = "12:00";

                String pattern = "dd/MM/yyHH:mm";
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                Date date = simpleDateFormat.parse(data + hora);

                // create a new Measurement with qualityKindRecord.get(header) and
                // unitRecord.get(header)
                System.out.println("Codigo=" + record.get(1));
                System.out.println("Measurement of " + qualityKindRecord.get(header));
                System.out.println("Value=" + record.get(header));

                String valueStr = record.get(header);
                float value = 0.0f;
                NumberFormat nf = NumberFormat.getInstance(Locale.forLanguageTag("pt-BR"));
                if (record.get(header).startsWith("<")) {
                    // below limit of detection
                } else if (record.get(header).startsWith(">")) {
                    // above limit of detection
                    value = nf.parse(valueStr.substring(1)).floatValue();
                } else {
                    value = nf.parse(valueStr).floatValue();
                }

                Load.addMeasurement(ontology, ":UNESP_" + codigo.replaceAll("\\s", "-"),
                        qualityKindRecord.get(header),
                        "http://qudt.org/vocab/unit/MilliGM-PER-L",
                        value,
                        date,
                        "http://purl.org/nemo/integradoce#IntegradoceUNESP");

            }
        }
    }
}
