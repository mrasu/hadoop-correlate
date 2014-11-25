#!/bin/sh
scp -r -P 2200 -i ~/.vagrant.d/insecure_private_key ./correlation/src/*  vagrant@localhost:build-hadoop/java_src/correlation
ssh vagrant@localhost -p 2200 -i ~/.vagrant.d/insecure_private_key  "~/build-hadoop/build.sh"
