#!/usr/bin/env bash

mvn clean install && scp target/delta-post-1.0-SNAPSHOT.jar run.sh 10.3.48.121:delta
ssh 10.3.48.121 delta/run.sh

