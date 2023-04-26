# The integradoce extract and transform application

This application performs the extraction of Water quality data from the Rio Doce Basin in various proprietary formats into triples that conform to the "doce water quality ontology" (<http://purl.org/nemo/doce>).

## Renova foundation format

One of the formats considered for extraction is that of Renova foundation as made available in 2021--2023 through <https://portal-de-monitoramento-rio-doce-fundacaorenova.hub.arcgis.com/pages/pa-download>.

It includes an [Excel file describing some metadata and context data] (<https://gis.fundacaorenova.org/portal/sharing/rest/content/items/cdd108af7c024914b8563644523c7485/data>) as well as data-point CSV files that can be downloaded by selecting filters at the website. The first tab of the Excel file (Detalhamento_pontos_PMQQS.xlsx) describes the various geographical points that are referenced in the data-point files. This first tab should be exported into a CSV file in Excel (named Detalhamento_pontos_PMQQS.csv) and placed with other data-point CSV files in the same folder for extraction and transformation.

## UNESP São Vicente format

Another format considered for extration is an adhoc format used by UNESP São Vicente researchers in the project, originally in an Excel file. Data was pre-processed into CSV files separating geographical point data and chemical concentration measurements. These files are used as input to the application in this repository.

## Instructions

Clone the repository and run `mvn package` in the repository folder.

Run the app with `java -jar target/etl-0.0.1-SNAPSHOT-jar-with-dependencies.jar <data_input_folder> <outputfile.ttl>`. See `sample_input_data` and `sample_output_data` folders for sample input and output respectively (output data compressed due to github size limits). The data is provided here for exemplification only; data sourced from Renova foundation comes with a disclaimer from that source ("é de sua inteira responsabilidade a interpretação e tratamento das informações deste arquivo").

## Further information

See <http://purl.org/nemo/doc/doce> for the ontology usage guide and complete documentation.

See <https://nemo.inf.ufes.br/en/projetos/integradoce/> for background on the overall project and publications.
