@set FLINK_HOME=C:\Proyectos_Local\master\flink-1.3.2-bin-hadoop27-scala_2.11\
set PATH=%PATH%;%FLINK_HOME%\bin
set FLINK_CONF_DIR=%FLINK_HOME%\conf

call flink.bat run -p 5 -c master2018.flink.VehicleTelematics -m localhost:6123 ..\target\VehicleTelematics-1.0-SNAPSHOT.jar
