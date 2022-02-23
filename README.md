# The integradoce extract and transform application

This application performs the extraction of Water quality data from the Rio Doce Basin in various proprietary formats into triples that conform to the "doce water quality ontology" (<http://purl.org/nemo/doce>). 

## Renova foundation 2021 format

One of the formats considered for extraction is that of Renova foundation as made available in 2021 through <https://portal-de-monitoramento-rio-doce-fundacaorenova.hub.arcgis.com/pages/pa-download>

It includes an [Excel file describing some metadata and context data] (<https://gis.fundacaorenova.org/portal/sharing/rest/content/items/cdd108af7c024914b8563644523c7485/data>) as well as data-point CSV files that can be downloaded by selecting filters at the website. The first tab of the Excel file (Detalhamento_pontos_PMQQS.xlsx) describes the various geographical points that are referenced in the data-point files. This first tab should be exported into a CSV file in Excel and placed with other data-point CSV files in the same folder for extraction and transformation.

## Further information

See <http://purl.org/nemo/doc/doce> for the ontology usage guide and complete documentation.

See <https://nemo.inf.ufes.br/en/projetos/integradoce/> for background on the overall project and publications.


