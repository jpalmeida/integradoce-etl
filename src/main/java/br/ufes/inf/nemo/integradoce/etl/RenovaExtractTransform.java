package br.ufes.inf.nemo.integradoce.etl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
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

public class RenovaExtractTransform {

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

    /**
     * Main entry point of the ETL process for Renova CSV files.
     * 
     * @param file
     * @param ontology
     * @throws IOException
     * @throws ParseException
     */
    public static void extractTransformRenova(File file, OWLOntology ontology) throws IOException, ParseException {
        LOGGER.info("Processing " + file.getName());

        Reader in = new FileReader(file);
        CSVParser parser = CSVFormat.EXCEL.withDelimiter(';').withFirstRecordAsHeader().parse(in);

        // since the Renova 2021 format consists of two different types of files
        // detect the type of file using the header
        if (parser.getHeaderMap().containsKey("Status_Condutividade")) {
            // this is the type of file that includes automatic sources (in-situ telemetry)
            extractTransformRenovaDataFileAutomaticSources(file, ontology);
        } else if (parser.getHeaderMap().containsKey("Status_Alcalinidade total")) {
            // type of file with manual samples (includes in-situ and ex-situ measurements)
            extractTransformRenovaDataFileManualSources(file, ontology);
        }
    }

    /**
     * ETL for Renova proprietary format for "automated" measurements.
     * 
     * Header and first line (example):
     * Matriz;TipoDeAmostra;CodigoDoPonto;Latitude;Longitude;DataAmostra;HoraAmostra;Cianobacteria
     * quali (µg/l);Status_Cianobacteria quali;Clorofila a (mg/L);Status_Clorofila
     * a;Clorofila a (µg/l);Status_Clorofila a;Condutividade
     * (mS/cm);Status_Condutividade;Condutividade (µS/cm);Status_Condutividade
     * (µS/cm);Nivel agua (cm);Status_Nivel agua;Oxigenio dissolvido saturado
     * (%);Status_Oxigenio dissolvido saturado;Oxigenio dissolvido
     * (%);Status_Oxigenio dissolvido (%);Oxigenio dissolvido (mg/L);Status_Oxigenio
     * dissolvido;Precipitacao (mm);Status_Precipitacao;Solidos suspensos totais
     * (mg/L);Status_Solidos suspensos totais;Temperatura ambiente
     * (ºC);Status_Temperatura ambiente;Temperatura da amostra
     * (ºC);Status_Temperatura da amostra;Turbidez
     * (NTU);Status_Turbidez;pH;Status_pH
     * Estacoes_automaticas;Telemetrico;RCA-01;-20.34624;-43.11147;01/08/2017;00:30;;6;;6;;6;;6;;6;121;1;;6;;6;;6;0;1;;6;15.8;1;;6;7;1;;6
     * 
     * @param file
     * @param ontology
     * @throws IOException
     * @throws ParseException
     */
    public static void extractTransformRenovaDataFileAutomaticSources(File file, OWLOntology ontology)
            throws IOException, ParseException {

        InputStream inputStream = RenovaExtractTransform.class.getResourceAsStream("/header-automatico.csv");
        BufferedReader inHeaderMapping = new BufferedReader(new InputStreamReader(
                inputStream));

        CSVParser parser = CSVFormat.EXCEL.withDelimiter(';').withFirstRecordAsHeader().parse(inHeaderMapping);
        List<String> headers = parser.getHeaderNames();

        // Iterable<CSVRecord> records =
        // CSVFormat.EXCEL.withDelimiter(';').withFirstRecordAsHeader().parse(inHeaderMapping);
        Iterator<CSVRecord> recordIterator = parser.iterator();
        CSVRecord qualityKindRecord = recordIterator.next();
        CSVRecord unitRecord = recordIterator.next();
        CSVRecord isMapped = recordIterator.next();

        Reader in = new FileReader(file);
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withDelimiter(';').withFirstRecordAsHeader().parse(in);
        for (CSVRecord record : records) {
            for (String header : headers) {
                if ((isMapped.get(header).equals("x"))
                        && (record.get(parser.getHeaderMap().get(header) + 1).contains("1")
                                || record.get(parser.getHeaderMap().get(header) + 1).contains("4")
                                || record.get(parser.getHeaderMap().get(header) + 1).contains("5"))) {
                    if (record.get(header).equals("")) {
                        LOGGER.warning("Void measurement of " + qualityKindRecord.get(header) + " with status "
                                + record.get(parser.getHeaderMap().get(header) + 1));
                        continue;
                    }
                    String codigo = record.get("CodigoDoPonto");
                    String data = record.get("DataAmostra");
                    String hora = record.get("HoraAmostra");
                    // String latitude = record.get("Latitude");
                    // String longitude = record.get("Longitude");

                    NumberFormat nf = NumberFormat.getInstance(Locale.US);
                    float value = nf.parse(record.get(header)).floatValue();

                    String pattern = "dd/MM/yyyyHH:mm";
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                    Date date = simpleDateFormat.parse(data + hora);

                    Load.addMeasurement(ontology, ":" + codigo.replaceAll("\\s", "-"), qualityKindRecord.get(header),
                            unitRecord.get(header), value, date, "http://purl.org/nemo/integradoce#Renova");

                    // create a new Measurement with qualityKindRecord.get(header) and
                    // unitRecord.get(header)
                    // System.out.println("Measurement of "+qualityKindRecord.get(header)+" with
                    // unit "+unitRecord.get(header));
                    // System.out.println("Value="+record.get(header));
                    //
                    // // check status in cell header+1
                    // System.out.println("Status="+record.get(parser.getHeaderMap().get(header)+1));
                    // só aproveitar se 1, 4 ou 5

                    // STATUS PARÂMETRO SIGNIFICADO DESCRIÇÃO
                    // 0 Dado não medido Esta situação ocorrerá quando não houver análise de algum
                    // parâmetro, que estava previsto pelo PMQQS.
                    // 1 Dado medido validado Esta situação ocorrerá quando a variável foi medida e
                    // validada.
                    // 2 Dado medido invalidado Esta situação ocorrerá quando a variável foi medida,
                    // porém invalidada.
                    // 3 Dado marcado com qualificador Esta situação ocorrerá quando o resultado
                    // receber algum tipo de qualificador.
                    // 4 Abaixo do limite de quantificação Esta situação ocorrerá quando o resultado
                    // da variável for inferior ao LQ.
                    // 5 Acima do limite máximo quantificável Esta situação ocorrerá quando o
                    // resultado da variável estiver acima do limite máximo quantificável.
                    // 6 Não se aplica Esta situação ocorrerá quando o parâmetro não estiver
                    // previsto no PMQQS para o determinado ambiente.
                }
            }
        }

    }

    /**
     * ETL for Renova proprietary format for "manual" measurements.
     * 
     * Matriz;TipoDeAmostra;CodigoDoPonto;Latitude;Longitude;DataAmostra;HoraAmostra;Alcalinidade
     * total (mgCaCO3/L);Status_Alcalinidade total;Aluminio dissolvido
     * (mg/L);Status_Aluminio dissolvido;Aluminio total (mg/L);Status_Aluminio
     * total;Aluminio (mg/kg);Status_Aluminio;Antimonio dissolvido
     * (mg/L);Status_Antimonio dissolvido;Antimonio total (mg/L);Status_Antimonio
     * total;Antimonio (mg/kg);Status_Antimonio;Area do amostrador (m²);Status_Area
     * do amostrador;Area molhada (m²);Areia fina (0,25 a 0,125 mm) (%);Status_Areia
     * fina (0,25 a 0,125 mm);Areia grossa (1 a 0,5 mm) (%);Status_Areia grossa (1 a
     * 0,5 mm);Areia media (0,5 a 0,25 mm) (%);Status_Areia media (0,5 a 0,25
     * mm);Areia muito fina (0,125 a 0,062 mm) (%);Status_Areia muito fina (0,125 a
     * 0,062 mm);Areia muito grossa (2 a 1 mm) (%);Status_Areia muito grossa (2 a 1
     * mm);Argila (0,00394 a 0,0002 mm) (%);Status_Argila (0,00394 a 0,0002
     * mm);Arsenio dissolvido (mg/L);Status_Arsenio dissolvido;Arsenio total
     * (mg/L);Status_Arsenio total;Arsenio (mg/kg);Status_Arsenio;Bario dissolvido
     * (mg/L);Status_Bario dissolvido;Bario total (mg/L);Status_Bario total;Bario
     * (mg/kg);Status_Bario;Berilio dissolvido (mg/L);Status_Berilio
     * dissolvido;Berilio total (mg/L);Status_Berilio total;Berilio total
     * (µg/l);Status_Berilio total (µg/l);Berilio (mg/kg);Status_Berilio;Biomassa
     * total de organismos (gPU/m²);Status_Biomassa total de organismos;Boro
     * dissolvido (mg/L);Status_Boro dissolvido;Boro total (mg/L);Status_Boro
     * total;Boro (mg/kg);Status_Boro;CE (I) 50% (48h) (%);Status_CE (I) 50%
     * (48h);CENO (I) (%);Status_CENO (I);CEO (I) (%);Status_CEO (I);CL (I) 50%
     * (96h) (%);Status_CL (I) 50% (96h);CLP (I) 50% (72h) (%);Status_CLP (I) 50%
     * (72h);Cadmio dissolvido (mg/L);Status_Cadmio dissolvido;Cadmio total
     * (mg/L);Status_Cadmio total;Cadmio (mg/kg);Status_Cadmio;Calcio dissolvido
     * (mg/L);Status_Calcio dissolvido;Calcio total (mg/L);Status_Calcio
     * total;Carbono organico dissolvido (mg/L);Status_Carbono organico
     * dissolvido;Carbono organico total (%);Status_Carbono organico total
     * (%);Carbono organico total (mg/L);Status_Carbono organico total;Ceriodaphnia
     * dubia (Toxico/Nao Toxico);Status_Ceriodaphnia dubia;Chumbo dissolvido
     * (mg/L);Status_Chumbo dissolvido;Chumbo total (mg/L);Status_Chumbo
     * total;Chumbo (mg/kg);Status_Chumbo;Cianeto livre (mg/L);Status_Cianeto
     * livre;Cianeto (mg/L);Status_Cianeto;Cianobac. quant (cel/mL);Status_Cianobac.
     * quant;Cloreto total (mg/L);Status_Cloreto total;Clorofila a
     * (µg/l);Status_Clorofila a;Cobalto dissolvido (mg/L);Status_Cobalto
     * dissolvido;Cobalto total (mg/L);Status_Cobalto total;Cobalto
     * (mg/kg);Status_Cobalto;Cobre dissolvido (mg/L);Status_Cobre dissolvido;Cobre
     * total (mg/L);Status_Cobre total;Cobre (mg/kg);Status_Cobre;Coeficiente de
     * variacao do controle (%);Status_Coeficiente de variacao do
     * controle;Concentracao relativa - Colby (mg/L);Status_Concentracao relativa -
     * Colby;Concentracao relativa - Einstein (mg/L);Status_Concentracao relativa -
     * Einstein;Condutividade in situ (µS/cm);Status_Condutividade in
     * situ;Condutividade insitu1 (µS/cm);Status_Condutividade insitu1;Condutividade
     * insitu2 (µS/cm);Status_Condutividade insitu2;Condutividade insitu3
     * (µS/cm);Status_Condutividade insitu3;Condutividade insitu4
     * (µS/cm);Status_Condutividade insitu4;Condutividade insitu5
     * (µS/cm);Status_Condutividade insitu5;Condutividade lab
     * (µS/cm);Status_Condutividade lab;Cor verdadeira (mgPt/L);Status_Cor
     * verdadeira;Cromo dissolvido (mg/L);Status_Cromo dissolvido;Cromo total
     * (mg/L);Status_Cromo total;Cromo (mg/kg);Status_Cromo;DBO
     * (mgO2/L);Status_DBO;DDD (ng/L);Status_DDD;DDD (µg/kg);Status_DDD (µg/kg);DDE
     * (ng/L);Status_DDE;DDE (µg/kg);Status_DDE (µg/kg);DDT (ng/L);Status_DDT;DDT
     * (µg/kg);Status_DDT (µg/kg);Daphnia Similis (Toxico/Nao Toxico);Status_Daphnia
     * Similis;Densidade numerica (ind/mm²);Status_Densidade numerica
     * (ind/mm²);Densidade numerica (org/ml);Status_Densidade numerica;Densidade
     * (ind/m²);Status_Densidade;Descarga solida de fundo - Colby
     * (t/dia);Status_Descarga solida de fundo - Colby;Descarga solida de fundo -
     * Einstein (t/dia);Status_Descarga solida de fundo - Einstein;Descarga solida
     * em suspensao - Colby (t/dia);Status_Descarga solida em suspensao -
     * Colby;Descarga solida em suspensao - Einstein (t/dia);Status_Descarga solida
     * em suspensao - Einstein;Descarga solida total - Colby (t/dia);Status_Descarga
     * solida total - Colby;Descarga solida total - Einstein (t/dia);Status_Descarga
     * solida total - Einstein;Dieldrin (ng/L);Status_Dieldrin;Dieldrin
     * (µg/kg);Status_Dieldrin (µg/kg);Dureza total (mgCaCO3/L);Status_Dureza
     * total;Endrin (µg/kg);Status_Endrin (µg/kg);Endrin
     * (µg/l);Status_Endrin;Escherichia coli (NMP/100mL);Status_Escherichia
     * coli;Estroncio total (mg/L);Status_Estroncio total;Estroncio
     * (mg/kg);Status_Estroncio;FT;Status_FT;Faixa 0.000 a 0.0156 mm - leito
     * (%);Status_Faixa 0.000 a 0.0156 mm - leito;Faixa 0.000 a 0.0156 mm -
     * suspensao (%);Status_Faixa 0.000 a 0.0156 mm - suspensao;Faixa 0.0156 a
     * 0.0625 mm - leito (%);Status_Faixa 0.0156 a 0.0625 mm - leito;Faixa 0.0156 a
     * 0.0625 mm - suspensao (%);Status_Faixa 0.0156 a 0.0625 mm - suspensao;Faixa
     * 0.0625 a 0.125 mm - leito (%);Status_Faixa 0.0625 a 0.125 mm - leito;Faixa
     * 0.0625 a 0.125 mm - suspensao (%);Status_Faixa 0.0625 a 0.125 mm -
     * suspensao;Faixa 0.125 a 0.250 mm - leito (%);Status_Faixa 0.125 a 0.250 mm -
     * leito;Faixa 0.125 a 0.250 mm - suspensao (%);Status_Faixa 0.125 a 0.250 mm -
     * suspensao;Faixa 0.250 a 0.500 mm - leito (%);Status_Faixa 0.250 a 0.500 mm -
     * leito;Faixa 0.250 a 0.500 mm - suspensao (%);Status_Faixa 0.250 a 0.500 mm -
     * suspensao;Faixa 0.500 a 1.000 mm - leito (%);Status_Faixa 0.500 a 1.000 mm -
     * leito;Faixa 0.500 a 1.000 mm - suspensao (%);Status_Faixa 0.500 a 1.000 mm -
     * suspensao;Faixa 1.000 a 2.000 mm - leito (%);Status_Faixa 1.000 a 2.000 mm -
     * leito;Faixa 1.000 a 2.000 mm - suspensao (%);Status_Faixa 1.000 a 2.000 mm -
     * suspensao;Faixa 2.000 a 4.000 mm - leito (%);Status_Faixa 2.000 a 4.000 mm -
     * leito;Faixa 2.000 a 4.000 mm - suspensao (%);Status_Faixa 2.000 a 4.000 mm -
     * suspensao;Faixa 4.000 a 8.000 mm - leito (%);Status_Faixa 4.000 a 8.000 mm -
     * leito;Faixa 4.000 a 8.000 mm - suspensao (%);Status_Faixa 4.000 a 8.000 mm -
     * suspensao;Faixa 8.000 a 16.000 mm - leito (%);Status_Faixa 8.000 a 16.000 mm
     * - leito;Faixa 8.000 a 16.000 mm - suspensao (%);Status_Faixa 8.000 a 16.000
     * mm - suspensao;Fenois totais (mg/L);Status_Fenois totais;Fenois totais
     * (mg/kg);Status_Fenois totais (mg/kg);Feoftina (µg/l);Status_Feoftina;Ferro II
     * (mg/L);Status_Ferro II;Ferro III (mg/L);Status_Ferro III;Ferro dissolvido
     * (mg/L);Status_Ferro dissolvido;Ferro total (mg/L);Status_Ferro total;Ferro
     * (mg/kg);Status_Ferro;Fluoreto (mg/L);Status_Fluoreto;Fosfato
     * (mg/L);Status_Fosfato;Fosforo dissolvido (mg/L);Status_Fosforo
     * dissolvido;Fosforo total (mg/L);Status_Fosforo total;Fosforo
     * (mg/kg);Status_Fosforo;Granulo (4 a 2 mm) (%);Status_Granulo (4 a 2 mm);HCH
     * (alfa-HCH) (ng/L);Status_HCH (alfa-HCH);HCH (alfa-HCH) (µg/kg);Status_HCH
     * (alfa-HCH) (µg/kg);HCH (beta-HCH) (ng/L);Status_HCH (beta-HCH);HCH (beta-HCH)
     * (µg/kg);Status_HCH (beta-HCH) (µg/kg);HCH (delta-HCH) (ng/L);Status_HCH
     * (delta-HCH);HCH (delta-HCH) (µg/kg);Status_HCH (delta-HCH)
     * (µg/kg);Imobilidade no controle (%);Status_Imobilidade no controle;Largura
     * (m);Status_Largura;Letalidade no controle (%);Status_Letalidade no
     * controle;Lindano (gama-HCH) (µg/kg);Status_Lindano (gama-HCH) (µg/kg);Lindano
     * (gama-HCH) (µg/l);Status_Lindano (gama-HCH);Magnesio dissolvido
     * (mg/L);Status_Magnesio dissolvido;Magnesio total (mg/L);Status_Magnesio
     * total;Manganes dissolvido (mg/L);Status_Manganes dissolvido;Manganes total
     * (mg/L);Status_Manganes total;Manganes (mg/kg);Status_Manganes;Mercurio
     * dissolvido (mg/L);Status_Mercurio dissolvido;Mercurio total
     * (mg/L);Status_Mercurio total;Mercurio (mg/kg);Status_Mercurio;Molibdenio
     * dissolvido (mg/L);Status_Molibdenio dissolvido;Molibdenio total
     * (mg/L);Status_Molibdenio total;Molibdenio (mg/kg);Status_Molibdenio;Niquel
     * dissolvido (mg/L);Status_Niquel dissolvido;Niquel total (mg/L);Status_Niquel
     * total;Niquel (mg/kg);Status_Niquel;Nitrato (mg/L);Status_Nitrato;Nitrito
     * (mg/L);Status_Nitrito;Nitrogenio amoniacal (mg/L);Status_Nitrogenio
     * amoniacal;Nitrogenio kjeldahl total (mg/L);Status_Nitrogenio kjeldahl
     * total;Nitrogenio kjeldahl total (mg/kg);Status_Nitrogenio kjeldahl total
     * (mg/kg);Nitrogenio organico (mg/L);Status_Nitrogenio organico;Nitrogenio
     * total (mg/kg);Status_Nitrogenio total;Nivel agua (cm);Status_Nivel
     * agua;Numero de taxons;Status_Numero de taxons;Numero total de
     * individuos;Status_Numero total de individuos;Oxigenio dissolvido in situ
     * (mg/L);Status_Oxigenio dissolvido in situ;Oxigenio dissolvido insitu1
     * (mg/L);Status_Oxigenio dissolvido insitu1;Oxigenio dissolvido insitu2
     * (mg/L);Status_Oxigenio dissolvido insitu2;Oxigenio dissolvido insitu3
     * (mg/L);Status_Oxigenio dissolvido insitu3;Oxigenio dissolvido insitu4
     * (mg/L);Status_Oxigenio dissolvido insitu4;Oxigenio dissolvido insitu5
     * (mg/L);Status_Oxigenio dissolvido insitu5;Oxigenio dissolvido saturado in
     * situ (%);Status_Oxigenio dissolvido saturado in situ;Oxigenio dissolvido
     * saturado insitu1 (%);Status_Oxigenio dissolvido saturado insitu1;Oxigenio
     * dissolvido saturado insitu2 (%);Status_Oxigenio dissolvido saturado
     * insitu2;Oxigenio dissolvido saturado insitu3 (%);Status_Oxigenio dissolvido
     * saturado insitu3;Oxigenio dissolvido saturado insitu4 (%);Status_Oxigenio
     * dissolvido saturado insitu4;Oxigenio dissolvido saturado insitu5
     * (%);Status_Oxigenio dissolvido saturado insitu5;Oxigenio dissolvido saturado
     * (%);Oxigenio dissolvido (mg/L);Polifosfato (mg/L);Status_Polifosfato;Potassio
     * dissolvido (mg/L);Status_Potassio dissolvido;Potencial redox in situ
     * (mV);Status_Potencial redox in situ;Potencial redox insitu1
     * (mV);Status_Potencial redox insitu1;Potencial redox insitu2
     * (mV);Status_Potencial redox insitu2;Potencial redox insitu3
     * (mV);Status_Potencial redox insitu3;Potencial redox insitu4
     * (mV);Status_Potencial redox insitu4;Potencial redox insitu5
     * (mV);Status_Potencial redox insitu5;Potencial redox lab (mV);Status_Potencial
     * redox lab;Prata dissolvido (mg/L);Status_Prata dissolvido;Prata total
     * (mg/L);Status_Prata total;Prata (mg/kg);Status_Prata;Profundidade de coleta
     * in situ (m);Status_Profundidade de coleta in situ;Profundidade maxima
     * (cm);Status_Profundidade maxima;Profundidade media (cm);Pseudokirchneriella
     * subcapitata (Toxico/Nao Toxico);Status_Pseudokirchneriella
     * subcapitata;Reproducao media no controle;Status_Reproducao media no
     * controle;Riqueza;Status_Riqueza;Salinidade in situ (PSU);Status_Salinidade in
     * situ;Salinidade insitu1 (PSU);Status_Salinidade insitu1;Salinidade insitu2
     * (PSU);Status_Salinidade insitu2;Salinidade insitu3 (PSU);Status_Salinidade
     * insitu3;Salinidade insitu4 (PSU);Status_Salinidade insitu4;Salinidade insitu5
     * (PSU);Status_Salinidade insitu5;Selenio dissolvido (mg/L);Status_Selenio
     * dissolvido;Selenio total (mg/L);Status_Selenio total;Selenio
     * (mg/kg);Status_Selenio;Silica dissolvida (mg/L);Status_Silica
     * dissolvida;Silte (0,062 a 0,00394 mm) (%);Status_Silte (0,062 a 0,00394
     * mm);Sodio dissolvido (mg/L);Status_Sodio dissolvido;Sodio total
     * (mg/L);Status_Sodio total;Solidos dissolvidos totais (mg/L);Status_Solidos
     * dissolvidos totais;Solidos sedimentaveis (ml/l);Status_Solidos
     * sedimentaveis;Solidos suspensos totais (mg/L);Status_Solidos suspensos
     * totais;Solidos totais (mg/L);Status_Solidos totais;Solidos
     * (%);Status_Solidos;Soma de PCB's (µg/kg);Status_Soma de PCB's (µg/kg);Soma de
     * PCB's (µg/l);Status_Soma de PCB's;Somatoria HAP's (µg/kg);Status_Somatoria
     * HAP's;Somatoria HAP’s (µg/l);Status_Somatoria HAP’s;Sulfato
     * (mg/L);Status_Sulfato;Sulfetos (como H2S nao dissociado)
     * (mg/L);Status_Sulfetos (como H2S nao dissociado);Sulfetos totais
     * (mg/L);Status_Sulfetos totais;TPH total (C8 - C40) (mg/kg);Status_TPH total
     * (C8 - C40);TPH total (C8 - C40) (µg/l);Status_TPH total (C8 - C40)
     * (µg/l);Taxa de crescimento em biomassa;Status_Taxa de crescimento em
     * biomassa;Temperatura ambiente in situ (ºC);Status_Temperatura ambiente in
     * situ;Temperatura ambiente insitu1 (ºC);Temperatura ambiente insitu3
     * (ºC);Temperatura ambiente insitu4 (ºC);Temperatura ambiente insitu5
     * (ºC);Temperatura da amostra in situ (ºC);Status_Temperatura da amostra in
     * situ;Temperatura da amostra insitu1 (ºC);Status_Temperatura da amostra
     * insitu1;Temperatura da amostra insitu2 (ºC);Status_Temperatura da amostra
     * insitu2;Temperatura da amostra insitu3 (ºC);Status_Temperatura da amostra
     * insitu3;Temperatura da amostra insitu4 (ºC);Status_Temperatura da amostra
     * insitu4;Temperatura da amostra insitu5 (ºC);Status_Temperatura da amostra
     * insitu5;Teor de carbonatos (%);Status_Teor de carbonatos;Teor de umidade
     * (%);Status_Teor de umidade;Toxicidade aguda (Toxico/Nao
     * Toxico);Status_Toxicidade aguda;Toxicidade cronica (Toxico/Nao
     * Toxico);Status_Toxicidade cronica;Transparencia da agua in situ
     * (m);Status_Transparencia da agua in situ;Transparencia da agua insitu1
     * (m);Turbidez in situ (NTU);Status_Turbidez in situ;Turbidez insitu1
     * (NTU);Status_Turbidez insitu1;Turbidez insitu2 (NTU);Status_Turbidez
     * insitu2;Turbidez insitu3 (NTU);Status_Turbidez insitu3;Turbidez insitu4
     * (NTU);Status_Turbidez insitu4;Turbidez insitu5 (NTU);Status_Turbidez
     * insitu5;Turbidez lab (NTU);Status_Turbidez lab;VC (%);Status_VC;Vanadio
     * dissolvido (mg/L);Status_Vanadio dissolvido;Vanadio total
     * (mg/L);Status_Vanadio total;Vanadio (mg/kg);Status_Vanadio;Vazao
     * (m³/s);Status_Vazao;Velocidade media (m/s);Zinco dissolvido
     * (mg/L);Status_Zinco dissolvido;Zinco total (mg/L);Status_Zinco total;Zinco
     * (mg/kg);Status_Zinco;alfa-Clordano (ng/L);Status_alfa-Clordano;alfa-Clordano
     * (µg/kg);Status_alfa-Clordano (µg/kg);gama-Clordano
     * (ng/L);Status_gama-Clordano;gama-Clordano (µg/kg);Status_gama-Clordano
     * (µg/kg);pH in situ;Status_pH in situ;pH insitu1;Status_pH insitu1;pH
     * insitu2;Status_pH insitu2;pH insitu3;Status_pH insitu3;pH insitu4;Status_pH
     * insitu4;pH insitu5;Status_pH insitu5;pH lab;Status_pH lab
     * Agua;P15;EBN
     * 01;-18.953517;-39.740729;08/08/2018;14:01;131;1;0.05;1,4;0.67;1;;6;0.001;1,4;0.001;1,4;;6;;6;;;6;;6;;6;;6;;6;;6;0.00154;1;0.00304;1;;6;0.01;1,4;0.01;1,4;;6;0.001;1,4;;6;1;1,4;;6;;6;2.8;1;4;1;;6;;6;;6;;6;;6;;6;0.001;1,4;0.001;1,4;;6;433;1;;6;4.2;1;;6;5.2;1;;6;0.01;1,4;0.01;1,4;;6;;6;;6;;6;15564;1;2.4;1;0.001;1,4;0.001;1,4;;6;0.001;1,4;0.001;1,4;;6;;6;;6;;6;55960;1;;6;;6;;6;;6;;6;46200;2;;6;0.01;1,4;0.01;1,4;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;6986.4;1;;6;;6;1.8;1,4;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;0.75;1,4;;6;;6;0.1;1,4;1.4;1;;6;0.21;1;0.3;1,4;0.01;1,4;0.01;1,4;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;1434;1;;6;0.01;1,4;0.0471;1;;6;0.0001;1,4;0.0001;1,4;;6;0.01;1,4;0.01;1,4;;6;0.01;1,4;0.01;1,4;;6;0.06;1,4;0.015;1;0.05;1,4;0.5;1,4;;6;;6;;6;;6;;6;;6;6;1;;6;;6;;6;;6;;6;99.6;1;;6;;6;;6;;6;;6;;;0.03;1,4;470;1;363;1;;6;;6;;6;;6;;6;;6;0.005;1,4;0.005;1,4;;6;0.15;1;;6;;;6;;6;;6;39.83;1;;6;;6;;6;;6;;6;0.001;1,4;0.001;1,4;;6;50;1,4;;6;12206;1;;6;32340;1;;6;42;1;;6;;6;;6;;6;;6;;6;1201;1;0.002;1,4;;6;;6;;6;;6;;6;;;;;24.5;1;;6;;6;;6;;6;;6;;6;;6;;6;;6;;6;;41.5;1;;6;;6;;6;;6;;6;11;1;;6;0.01;1,4;0.01;1,4;;6;;6;;0.01;1,4;0.01;1,4;;6;;6;;6;;6;;6;8;1;;6;;6;;6;;6;;6;7.1;1
     * 
     * @param file
     * @param ontology
     * @throws IOException
     * @throws ParseException
     */
    public static void extractTransformRenovaDataFileManualSources(File file, OWLOntology ontology)
            throws IOException, ParseException {

        // reads a mapping file that includes quality kinds corresponding to a certain
        // header, units and an indication
        // on whether the column is incljuded in the mapping

        InputStream inputStream = RenovaExtractTransform.class.getResourceAsStream("/header-manual.csv");
        BufferedReader inHeaderMapping = new BufferedReader(new InputStreamReader(
                inputStream));

        CSVParser parser = CSVFormat.EXCEL.withDelimiter(';').withFirstRecordAsHeader().parse(inHeaderMapping);
        List<String> headers = parser.getHeaderNames();

        Iterator<CSVRecord> recordIterator = parser.iterator();
        CSVRecord qualityKindRecord = recordIterator.next();
        CSVRecord unitRecord = recordIterator.next();
        CSVRecord isMapped = recordIterator.next();

        // having read the mapping file, let's process the actual data

        Reader in = new FileReader(file);
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withDelimiter(';').withFirstRecordAsHeader().parse(in);
        // for each line of the CSV file
        for (CSVRecord record : records) {
            // for each column
            for (String header : headers) {

                if ((record.get(0).equals("Agua") || record.get(0).equals("Descarga_liquida")) &&

                        (isMapped.get(header).equals("x"))) {
                    // só Matriz Agua?
                    // Matriz=Descarga_liquida Vazao (m³/s)
                    // Matriz=Ecotoxi_Agua

                    // check status in cell header+1
                    // if the next column does not start with "Status" or
                    // if it does and its value is 1, 4, 5

                    String nextHeaderName = parser.getHeaderNames().get(parser.getHeaderMap().get(header) + 1);

                    if ((!nextHeaderName.startsWith("Status"))
                            || (record.get(nextHeaderName).contains("1"))
                            || (record.get(nextHeaderName).contains("4"))
                            || (record.get(nextHeaderName).contains("5"))) {
                        // create a new Measurement with qualityKindRecord.get(header) and
                        // unitRecord.get(header)
                        // System.out.println("Header: "+header);
                        // System.out.println("Measurement of " + qualityKindRecord.get(header) + " with
                        // unit "
                        // + unitRecord.get(header));
                        // System.out.println("Value=" + record.get(header));
                        // System.out.println("Status=" + record.get(parser.getHeaderMap().get(header) +
                        // 1));
                        // System.out.println("Column number="+parser.getHeaderMap().get(header) + 1);
                        // System.out.println("Status'="+record.get(parser.getHeaderNames().get(parser.getHeaderMap().get(header)
                        // + 1)));
                        // STATUS PARÂMETRO SIGNIFICADO DESCRIÇÃO
                        // 0 Dado não medido Esta situação ocorrerá quando não houver análise de algum
                        // parâmetro, que estava previsto pelo PMQQS.
                        // 1 Dado medido validado Esta situação ocorrerá quando a variável foi medida e
                        // validada.
                        // 2 Dado medido invalidado Esta situação ocorrerá quando a variável foi medida,
                        // porém invalidada.
                        // 3 Dado marcado com qualificador Esta situação ocorrerá quando o resultado
                        // receber algum tipo de qualificador.
                        // 4 Abaixo do limite de quantificação Esta situação ocorrerá quando o resultado
                        // da variável for inferior ao LQ.
                        // 5 Acima do limite máximo quantificável Esta situação ocorrerá quando o
                        // resultado da variável estiver acima do limite máximo quantificável.
                        // 6 Não se aplica Esta situação ocorrerá quando o parâmetro não estiver
                        // previsto no PMQQS para o determinado ambiente.

                        String codigo = record.get("CodigoDoPonto");
                        String data = record.get("DataAmostra");
                        String hora = record.get("HoraAmostra");
                        // String latitude = record.get("Latitude");
                        // String longitude = record.get("Longitude");

                        NumberFormat nf = NumberFormat.getInstance(Locale.US);
                        float value = nf.parse(record.get(header)).floatValue();

                        String pattern = "dd/MM/yyyyHH:mm";
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                        Date date = simpleDateFormat.parse(data + hora);

                        Load.addMeasurement(ontology, ":" + codigo.replaceAll("\\s", "-"),
                                qualityKindRecord.get(header),
                                unitRecord.get(header), value, date, "http://purl.org/nemo/integradoce#Renova");

                    }
                }
            }
        }
    }
}
