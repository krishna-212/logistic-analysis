# logistic-analysis
Stream logistic file to BQ

## Set Up
Follow the
[Getting started with Google Cloud Dataflow](../README.md)
###	[Set up your Java Development Environment](https://cloud.google.com/java/docs/setup)

##	Clone this repository:

	git clone git@github.com:krishna-212/logistic-analysis.git
	cd logistic-analysis/streaming-dataflow/LogisticStreaming

	Build the jar using below commands:
	sh
	# Build and package the application as an uber-jar file.
	mvn clean package
	
	# (Optional) Note the size of the uber-jar file compared to the original.
	ls -lh target/RealTimeStreamingDF-1.0.jar```

##	There are multiple ways to run the jar
	
	1. Using java jar command
		java -jar RealTimeStreamingDF-1.0.jar --runner=DataflowRunner --project= --stagingLocation= --tempLocation=  --region=us-central1 --maxNumWorkers=4 --workerMachineType=n1-standard-4 --projectId= --tableName --filePath --errorBucket --jobName=
		

	2.Create template 
		mvn -Pdataflow-runner compile exec:java \
      -Dexec.mainClass= \
      -Dexec.cleanupDaemonThreads=false \
      -Dexec.args="--project= \
      --stagingLocation= \
      --templateLocation= \
      --tempLocation=g \
      --runner=DataflowRunner \
      --region=
      
      Run gcloud template command
      gcloud dataflow jobs run job-name  --gcs-location=gs://Template-path --param
      
      
    3.Flex Template job with a custom Docker image
    	 To run a template, you need to create a *template spec* file containing all the 
    	 necessary information to run the job, such as the SDK information and metadata.
    	 The [`metadata.json`](metadata.json) file contains additional information for
    	 the template such as the `name`, `description`, and input `parameters` field.
    	 I have added it in github repo
    	 
    	 export TEMPLATE_PATH="gs://location/${BUCKET}/streaming-beam-df.json"
    	 export TEMPLATE_IMAGE="gcr.io/${PROJECT_ID}/streaming-logistic-beam-df:latest"
    	 
    	 gcloud dataflow flex-template build $TEMPLATE_PATH \
    	 --image-gcr-path "$TEMPLATE_IMAGE" \
    	 --sdk-language "JAVA" \
    	 --flex-template-base-image JAVA11 \
    	 --metadata-file "metadata.json" \
    	 --jar "target/RealTimeStreamingDF-1.0.jar" \
    	 --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.cloud.dataflow.realtime.pipeline.StreamingPipeline"
    
    	 gcloud dataflow flex-template run "streaming-analytics-`date +%Y%m%d-%H%M%S`" \
    	 --template-file-gcs-location "$TEMPLATE_PATH" \	--parameters tableName="${TABLE_NAME}" \
    	 --parameters filePath="gs://${BUCKET}/sample_test_data/*" \
    	 --parameters errorBucket="gs://${BUCKET}/error_data/" \
    	 --region "${REGION}



##	NOTE
Just need to run the Dataflow Streaming Job, no need to setup anything else. As soon as you insert new files,DF automatically picks it up. In every 5 mins, Dataflow checks for new files and stream that record to BigQuery.
	
