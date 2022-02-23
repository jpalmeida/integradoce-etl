package br.ufes.inf.nemo.integradoce.etl;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

/**
 * Extracts and transforms to triples conforming to http://purl.org/nemo/doce
 * ontology.
 *
 */
public class App {
	private final static Logger LOGGER = Logger.getLogger("ETL");

	public App() {

	}



	public void extractTransformGeographicPoints(String csvFilePath, OWLOntology ontology)
			throws ParseException, IOException {

		// TODO add geographical information such as city, municipality, river basin

		Reader in = new FileReader(csvFilePath);

		// header:
		// MONITORAMENTO;CODIGO_PONTO;NOME_PONTO;DESCRICAO_PONTO;TIPO_ESTACAO;AMBIENTE;CORPO_HIDRICO;SUB_BACIA;BACIA;MUNICIPIO;ESTADO;LONGITUDE;LATITUTE;UTM_X;UTM_Y;PROJECAO;DATUM;ALTITUDE;CODIGO_HIDROWEB;CODIGO_PONTO_ANTERIOR
		// example row:
		// Aguas Interiores;RVD-03;Mariana - Dique S3;No vertedouro do Dique S3.
		// Coincide com o antigo ponto: RDC-124. Atingido pelo rejeito;Manual;Agua Doce
		// Lotico;Corrego Santarém;Córrego Santarém;Rio

		Iterable<CSVRecord> records = CSVFormat.EXCEL.withDelimiter(';').withFirstRecordAsHeader().parse(in);
		for (CSVRecord record : records)
		{
			String codigo = record.get("CODIGO_PONTO");
			String nome = record.get("NOME_PONTO");
			String descricao = record.get("DESCRICAO_PONTO");
			String latitude = record.get("LATITUTE"); // note the typo in LATITUDE... that's part of the Renova format
			String longitude = record.get("LONGITUDE");

			NumberFormat nf = NumberFormat.getInstance(Locale.forLanguageTag("pt-BR"));
			Load.addGeographicPoint(ontology, ":" + codigo.replaceAll("\\s", "-"), nf.parse(latitude).floatValue(),
					nf.parse(longitude).floatValue(), descricao, nome);
		}
	}




	public static void main(String[] args) throws OWLOntologyCreationException, OWLOntologyStorageException,
			SecurityException, IOException, ParseException {

		// setup logger
		FileHandler fh = new FileHandler("etl.log", true);
		System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tr] %4$s: %5$s%n");
		fh.setFormatter(new SimpleFormatter());
		LOGGER.addHandler(fh);

		App app = new App();

		// load ontology from web
		LOGGER.info("Loading doce ontology...");
		OWLOntology ontology = Load.loadDoce();
		LOGGER.info("Loaded doce ontology.");

		// extract geographic points from metadata file
		app.extractTransformGeographicPoints(
				"/Users/jpalmeida/Dropbox/Documents/ufes/projetos/riodoce/dados_renova_2021/Detalhamento_pontos_PMQQS.csv",
				ontology);

		Load.addWellKnownEntities(ontology);

		// take all csv files in the given folder and their immediate subfolders
		File folder = new File("/Users/jpalmeida/Dropbox/Documents/ufes/projetos/riodoce/dados_renova_2021");
		for (File f : folder.listFiles())
		{
			// if the file is a directory, list the files and extract and transform the csv
			// ones
			if (f.isDirectory())
			{
				for (File f2 : f.listFiles())
				{
					if (f2.getName().endsWith(".csv"))
						RenovaExtractTransform.extractTransformRenova(f2, ontology);
				}
			} else if (f.getName().endsWith(".csv"))
			RenovaExtractTransform.extractTransformRenova(f, ontology);
		}

		// extract geographic points from metadata file
		UnespExtractTransform.extractTransformGeographicPointsUNESP(
				"/Users/jpalmeida/Dropbox/Documents/ufes/projetos/riodoce/dados_unesp_2021/pontos.csv", ontology);

		// take all csv files in the given folder and their immediate subfolders
		folder = new File("/Users/jpalmeida/Dropbox/Documents/ufes/projetos/riodoce/dados_unesp_2021/agua");
		for (File f : folder.listFiles())
		{
			// if the file is a directory, list the files and extract and transform the csv
			// ones
			if (f.isDirectory())
			{
				for (File f2 : f.listFiles())
				{
					if (f2.getName().endsWith(".csv"))
					UnespExtractTransform.extractTransformUNESP(f2, ontology);
				}
			} else if (f.getName().endsWith(".csv"))
			UnespExtractTransform.extractTransformUNESP(f, ontology);
		}

		LOGGER.info("Saving extracted and transformed data points...");
		// serialize the ttl file again, for later loading into triple store
		Load.save(ontology);

		LOGGER.info("Saved extracted and transformed data points.");

	}


}
