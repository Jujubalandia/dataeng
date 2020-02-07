# Data engineering case studyand pipeline flow 

Docker image for **python3** on alpine linux.
Included some libraries for dealing with http requests, google cloud SDK, mainly google cloud storage, bigquery, dataflow, composer :
* requests: 2.22.0
* google-cloud-storage:1.23.0
* google-cloud-logging:1.14.0
* google-cloud-error-reporting:0.33.0

#  How do I build this application
In the root directory of this project you can run the following command:

**docker build -t btest2 -f Dockerfile . --no-cache**

This will start a docker build.

#  How do I run this application

When the build has been completed and you want to run this as is, you can just run the following command

**docker run btest2:latest**

This will start a direct shell into the docker.
However, it is better to build this image and use it as a reference in an other dockerfile



