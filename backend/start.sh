#!/bin/bash
while ! nc -z kafka 29092; do sleep 3; done
node ./src/index.js