## Prerequisites
- Installed FLINK version 1.3.2
- Maven

## Build project
> mvn clean package -Pbuild-jar


## Run project
In the folder of project:
> flink run -p 10 -c master2018.flink.VehicleTelematics target/$YOUR_JAR_FILE $PATH_TO_INPUT_FILE $PATH_TO_OUTPUT_FOLDER 

* flink = root where flink is installed
