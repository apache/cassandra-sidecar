#!/usr/bin/env bash

# Script to start minikube locally and push the containers to the local registry
# we need to use the hyperkit driver or we can't use the registry

minikube start --insecure-registry "192.168.0.0/16" --insecure-registry "10.0.0.0/24"  --memory 8G --cpus=4 --vm=true
minikube addons enable registry
minikube addons enable dashboard
minikube addons list

kubectl get all

echo "Be sure to configure your docker registry to allow for insecure image uploads:"

echo ""
echo "{"
echo " \"insecure-registries\": [\"$(minikube ip):5000\"]  "
echo "}"

echo "Ensure your docker configuration (Docker preferences -> Docker Engine) allows for insecure pushes then press enter to publish the test containers."

read var

./gradlew buildAll pushAll

echo "To allow the tests to connect to your containers, please run the following:"

echo "minikube tunnel"

echo "Remember to do this before running integration tests!"