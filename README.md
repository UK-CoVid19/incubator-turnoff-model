# incubator-turnoff-model
Analysis of reat-time incubator temperature data.

## Description
The program reads real-time temperature data and analyses if the incubator (a) stable, (b) being turned off, or (c) other.
Runs on google cloud dataflow and builds into a docker container.


## Build & Deploy

From the command line:
 - `gcloud builds submit --tag eu.gcr.io/neon-vigil-312408/gcf/europe-west2/temperature_analysis/image .`
 - `gcloud dataflow flex-template build gs://temperature-analysis-data/templates/temperature_analysis.json --image eu.gcr.io/neon-vigil-312408/gcf/europe-west2/temperature_analysis/image@sha256:9e3294037620f595ead313c065cdaecc6585a130737dd48f6458969105f65a1c --sdk-language "PYTHON" --metadata-file metadata.json` (image is the one just built)
 - `` gcloud dataflow flex-template run "temperature-analysis-`date +%Y%m%d-%H%M%S`" --template-file-gcs-location "gs://temperature-analysis-data/templates/temperature_analysis.json" --region europe-west1``

## Logging 

 - retrieve logs: `gsutil cp gs://dataflow-staging-europe-west1-666798192016/staging/template_launches/2022-05-31_07_42_57-3979253926110456900/console_logs .` where the filename is the pipeline run name