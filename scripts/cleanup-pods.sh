#!/usr/bin/env bash

echo "Existing pods and services"
kubectl get pods
kubectl get services

echo "Cleaning up pods:"
kubectl get pods | awk '{print $1}' | egrep '^cassandra-' | xargs -n 1 kubectl delete pod

echo "Cleaning up services:"
kubectl get services | awk '{print $1}' | egrep '^cassandra-' | xargs -n 1 kubectl delete service

