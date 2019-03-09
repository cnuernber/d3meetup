#!/bin/bash

mkdir -p data

pushd data

wget https://s3.us-east-2.amazonaws.com/tech.public.data/brian-lehman-strava.zip

unzip brian-lehman-strava.zip

popd
