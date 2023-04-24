package br.ufes.inf.nemo.integradoce.etl;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassAssertionAxiom;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLDataPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLLiteral;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;
import org.semanticweb.owlapi.model.PrefixManager;
import org.semanticweb.owlapi.util.DefaultPrefixManager;
import org.semanticweb.owlapi.vocab.OWL2Datatype;


public class Load {
    

    // following examples in
    // https://github.com/owlcs/owlapi/blob/version4/contract/src/test/java/org/semanticweb/owlapi/examples/Examples.java

    private static OWLOntologyManager manager;
    private static OWLDataFactory dataFactory;

    /**
     * Prefixes are required for all operations that will obtain instances of
     * OWLClass, OWLNamedIndividual
     */
    private static PrefixManager rdfspm, gufopm, docepm, integradocepm, wgspm;

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
        wgspm = new DefaultPrefixManager(null, null, "http://www.w3.org/2003/01/geo/wgs84_pos#");
        riodoce = dataFactory.getOWLNamedIndividual(":RioDoce", docepm);
    }

    /**
     * 
     * @param ontology
     */
	// public static void addInstances(OWLOntology ontology) {
	// 	// Now we use the prefix manager and just specify an abbreviated IRI
	// 	OWLClass river = dataFactory.getOWLClass(":River", docepm);
	// 	OWLNamedIndividual amazonas = dataFactory.getOWLNamedIndividual(":Amazonas", integradocepm);
	// 	OWLClassAssertionAxiom classAssertion = dataFactory.getOWLClassAssertionAxiom(river, amazonas);
	// 	manager.addAxiom(ontology, classAssertion);
    // }


    /**
     * Loads "doce" from <http://purl.org/nemo/doce>.
     * 
     * @return
     * @throws OWLOntologyCreationException
     * @throws OWLOntologyStorageException
     */
	public static OWLOntology loadDoce() throws OWLOntologyCreationException, OWLOntologyStorageException {
    	// based on
    	// https://github.com/owlcs/owlapi/blob/version4/contract/src/test/java/org/semanticweb/owlapi/examples/Examples.java

		IRI ontologyIRI = IRI.create("http://purl.org/nemo/doce");
		OWLOntology ontology = manager.loadOntology(ontologyIRI);
		return ontology;
	}

    /**
     * Add well-known entities that will be required in the extract and transform processes.
     * 
     * Currently, adds Renova and IntegradoceUNESP (instances of doce:Agent)
     * 
     * @param ontology
     */
	public static void addWellKnownEntities(OWLOntology ontology) {
                // add agent for Renova
		OWLClass agentClass = dataFactory.getOWLClass(":Agent", docepm);
		OWLClassAssertionAxiom classAssertion = dataFactory.getOWLClassAssertionAxiom(agentClass,
				dataFactory.getOWLNamedIndividual("Renova", integradocepm));
		manager.addAxiom(ontology, classAssertion);

                // add agent for UNESP team
		classAssertion = dataFactory.getOWLClassAssertionAxiom(agentClass,
				dataFactory.getOWLNamedIndividual("IntegradoceUNESP", integradocepm));
                manager.addAxiom(ontology, classAssertion);
        }

    /**
     * Adds a geographic point (instance of doce:GeographicPoint) 
     * 
     * Example of a doce:GeographicPoint where the sampling and in-situ measurements took place:
     * 
     * :RCA-01 rdf:type owl:NamedIndividual ,
     *          doce:GeographicPoint ;
     * 			wgs:lat "-20.3471"^^xsd:float ;
     * 	                wgs:long "-43.1127"^^xsd:float ;
     * 	        rdfs:comment "Ponte férrea sobre o rio do Carmo, em Acaiaca (MG). Não atingido pelo rejeito. Área de pastagem." ;
     * 	        rdfs:label "Acaiaca - Carmo 01" .
     */
	public static void addGeographicPoint(OWLOntology ontology, String pointIRI, float lat, float lon, String commentValue,
			String labelValue) {

		OWLClass geopointClass = dataFactory.getOWLClass(":GeographicPoint", docepm);
		OWLNamedIndividual geopoint = dataFactory.getOWLNamedIndividual(pointIRI, integradocepm);
		OWLClassAssertionAxiom classAssertion = dataFactory.getOWLClassAssertionAxiom(geopointClass, geopoint);
		manager.addAxiom(ontology, classAssertion);

		// OWLDataProperty hasLatitude = dataFactory.getOWLDataProperty(":hasLatitude", docepm);
		// OWLDataProperty hasLongitude = dataFactory.getOWLDataProperty(":hasLongitude", docepm);
		
                OWLDataProperty wgsLat = dataFactory.getOWLDataProperty(":lat", wgspm);
                OWLDataProperty wgsLong = dataFactory.getOWLDataProperty(":long", wgspm);

		OWLDataProperty commentProperty = dataFactory.getOWLDataProperty(":comment", rdfspm);
		OWLDataProperty labelProperty = dataFactory.getOWLDataProperty(":label", rdfspm);

		OWLDataPropertyAssertionAxiom dataPropertyAssertion = dataFactory.getOWLDataPropertyAssertionAxiom(wgsLat,
				geopoint, lat);
		manager.addAxiom(ontology, dataPropertyAssertion);

		dataPropertyAssertion = dataFactory.getOWLDataPropertyAssertionAxiom(wgsLong, geopoint, lon);
		manager.addAxiom(ontology, dataPropertyAssertion);

		dataPropertyAssertion = dataFactory.getOWLDataPropertyAssertionAxiom(commentProperty, geopoint, commentValue);
		manager.addAxiom(ontology, dataPropertyAssertion);

		dataPropertyAssertion = dataFactory.getOWLDataPropertyAssertionAxiom(labelProperty, geopoint, labelValue);
		manager.addAxiom(ontology, dataPropertyAssertion);

//        OWLDatatype integerDatatype = factory.getOWLDatatype(OWL2Datatype.XSD_INTEGER.getIRI());
//        // Create a typed literal. We type the literal "51" with the datatype
//        OWLLiteral literal = factory.getOWLLiteral("51", integerDatatype);
//        // Create the property assertion and add it to the ontology
//        OWLAxiom ax = factory.getOWLDataPropertyAssertionAxiom(hasAge, john, literal);

	}

    public static void addMeasurement(OWLOntology ontology, String geopointCode, String qualityKindIRI, String unitIRI,
            float value, Date date, String agentIRI) {

        // :WaterTransparencyMeasurement314020-2017-1 rdf:type owl:NamedIndividual ,
        // doce:Measurement ;
        // doce:locatedIn :RCA-01 ;
        // doce:measured doce:RioDoce ;
        // doce:measuredQualityKind doce:SecchiDiskTransparency ;
        // doce:expressedIn unit:M ;
        // gufo:hasBeginPointInXSDDateTimeStamp
        // "2009-08-17T14:24:00-03:00"^^xsd:dateTimeStamp ;
        // gufo:hasEndPointInXSDDateTimeStamp
        // "2009-08-17T14:24:00-03:00"^^xsd:dateTimeStamp ;
        // gufo:hasQualityValue "0.43"^^xsd:double .

        OWLClass qualityKindClass = dataFactory.getOWLClass(qualityKindIRI);
        OWLClass measurementClass = dataFactory.getOWLClass(":Measurement", docepm);
        IRI measurementIRI = IRI.getNextDocumentIRI(
                integradocepm.getDefaultPrefix() + qualityKindClass.getIRI().getShortForm() + "Measurement");
        OWLNamedIndividual measurement = dataFactory.getOWLNamedIndividual(measurementIRI);
        OWLClassAssertionAxiom classAssertion = dataFactory.getOWLClassAssertionAxiom(measurementClass, measurement);
        // System.out.println(classAssertion);
        manager.addAxiom(ontology, classAssertion);

        OWLObjectProperty locatedIn = dataFactory.getOWLObjectProperty(":locatedIn", docepm);
        OWLNamedIndividual geopoint = dataFactory.getOWLNamedIndividual(geopointCode, integradocepm);
        OWLObjectPropertyAssertionAxiom objPropertyAssertion = dataFactory.getOWLObjectPropertyAssertionAxiom(locatedIn,
                measurement, geopoint);
        manager.addAxiom(ontology, objPropertyAssertion);

        OWLObjectProperty measuredQualityKind = dataFactory.getOWLObjectProperty(":measuredQualityKind", docepm);
        OWLNamedIndividual qualityKind = dataFactory.getOWLNamedIndividual(qualityKindIRI);
        objPropertyAssertion = dataFactory.getOWLObjectPropertyAssertionAxiom(measuredQualityKind, measurement,
                qualityKind);
        manager.addAxiom(ontology, objPropertyAssertion);

        OWLObjectProperty expressedIn = dataFactory.getOWLObjectProperty(":expressedIn", docepm);
        OWLNamedIndividual unit = dataFactory.getOWLNamedIndividual(unitIRI);
        objPropertyAssertion = dataFactory.getOWLObjectPropertyAssertionAxiom(expressedIn, measurement, unit);
        manager.addAxiom(ontology, objPropertyAssertion);

        OWLDataProperty hasQualityValue = dataFactory.getOWLDataProperty(":hasQualityValue", gufopm);
        OWLDataPropertyAssertionAxiom dataPropertyAssertion = dataFactory
                .getOWLDataPropertyAssertionAxiom(hasQualityValue, measurement, value);
        manager.addAxiom(ontology, dataPropertyAssertion);

        if (date != null) {
            OWLDataProperty hasBeginPointInXSDDateTimeStamp = dataFactory
                    .getOWLDataProperty(":hasBeginPointInXSDDateTimeStamp", gufopm);
            OWLLiteral ol = dataFactory.getOWLLiteral(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(date),
                    OWL2Datatype.XSD_DATE_TIME_STAMP);
            dataPropertyAssertion = dataFactory.getOWLDataPropertyAssertionAxiom(hasBeginPointInXSDDateTimeStamp,
                    measurement, ol);
            manager.addAxiom(ontology, dataPropertyAssertion);

            OWLDataProperty hasEndPointInXSDDateTimeStamp = dataFactory.getOWLDataProperty(
                    ":hasEndPointInXSDDateTimeStamp",
                    gufopm);
            ol = dataFactory.getOWLLiteral(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(date),
                    OWL2Datatype.XSD_DATE_TIME_STAMP);
            dataPropertyAssertion = dataFactory.getOWLDataPropertyAssertionAxiom(hasEndPointInXSDDateTimeStamp,
                    measurement,
                    ol);
            manager.addAxiom(ontology, dataPropertyAssertion);
        }

        // FIXME treat status
        // FIXME add doce:measured

        OWLObjectProperty participatedIn = dataFactory.getOWLObjectProperty(":participatedIn", gufopm);
        OWLNamedIndividual agent = dataFactory.getOWLNamedIndividual(agentIRI);
        objPropertyAssertion = dataFactory.getOWLObjectPropertyAssertionAxiom(participatedIn, agent, measurement);
        manager.addAxiom(ontology, objPropertyAssertion);

    }



	public static void save(OWLOntology ontology, String pathname) throws OWLOntologyStorageException {
		File file = new File(pathname);
		manager.saveOntology(ontology, IRI.create(file.toURI()));
	}


}
