# project-fortis-spark

A repository for all spark jobs running on fortis

## What's this? ##

This project contains demos of the various Spark Streaming data processing methods for Project Fortis:

- Ingesting Instagram pictures and analyzing them using Cognitive Services.
- Ingesting Twitter tweets and extracting locations using Open Street Map.

Run it via:

```sh
# set up all the requisite environment variables
export INSTAGRAM_AUTH_TOKEN="..."
export OXFORD_VISION_TOKEN="..."
export TWITTER_CONSUMER_KEY="..."
export TWITTER_CONSUMER_SECRET="..."
export TWITTER_ACCESS_TOKEN="..."
export TWITTER_ACCESS_TOKEN_SECRET="..."

# compile scala, run tests, build fat jar
sbt assembly

# run on spark
spark-submit --class DemoInstagramAndLocations --master local[4] target/scala-2.11/project-fortis-spark-assembly-0.0.1.jar instagram
spark-submit --class DemoInstagramAndLocations --master local[4] target/scala-2.11/project-fortis-spark-assembly-0.0.1.jar twitter
```