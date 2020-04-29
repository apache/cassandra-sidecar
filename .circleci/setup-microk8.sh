#!/bin/bash

set -v

sudo snap install microk8s --classic

sudo microk8s status --wait-ready
sudo microk8s enable registry
sudo microk8s status --wait-ready
sudo microk8s status -a registry -wait-ready

curl http://localhost:32000/v2/_catalog

sudo mkdir -p ~/.kube
sudo chown -R circleci ~/.kube

sudo microk8s config > ~/.kube/config
sudo chown -R circleci ~/.kube

cat ~/.kube/config

sudo microk8s kubectl get all
sudo iptables -P FORWARD ACCEPT