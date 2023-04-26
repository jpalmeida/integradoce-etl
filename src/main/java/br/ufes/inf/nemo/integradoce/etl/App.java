package br.ufes.inf.nemo.integradoce.etl;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

/**
 * Extracts and transforms to triples conforming to http://purl.org/nemo/doce
 * ontology.
 *
 * See source repository at <https://github.com/jpalmeida/integradoce-etl>
 * and project documentation at <https://nemo.inf.ufes.br/en/projetos/integradoce/>.
 * 
 */
public class App {
	private final static Logger LOGGER = Logger.getLogger("ETL");

	public static void main(String[] args) throws OWLOntologyCreationException, OWLOntologyStorageException,
			SecurityException, IOException, ParseException {

		if (args.length!=2) 
		{
			System.err.println("Usage: App <data_input_folder> <outputfile.ttl>");
			System.err.println("The <data_input_folder> should have two subfolders 'dados_renova' and 'dados_unesp' with respective CSV files.");
			return;
		}

		String baseDir = args[0];

		// setup logger
		FileHandler fh = new FileHandler("etl.log", true);
		System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tr] %4$s: %5$s%n");
		fh.setFormatter(new SimpleFormatter());
		LOGGER.addHandler(fh);

		// load ontology from web
		LOGGER.info("Loading doce ontology...");
		OWLOntology ontology = Load.loadDoce();
		LOGGER.info("Loaded doce ontology.");

		// extract geographic points from metadata file
		RenovaExtractTransform.extractTransformGeographicPoints(
				baseDir+
				"/dados_renova/Detalhamento_pontos_PMQQS.csv",
				ontology);

		Load.addWellKnownEntities(ontology);

		File folder;

		// take all csv files in the given folder and their immediate subfolders
		folder = new File(baseDir+"/dados_renova");
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
			baseDir+"/dados_unesp/pontos.csv", ontology);

		// take all csv files in the given folder and their immediate subfolders
		folder = new File(baseDir+"/dados_unesp/agua");
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
		Load.save(ontology,args[1]);

		LOGGER.info("Saved extracted and transformed data points.");

	}


}
