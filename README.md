# gx4ki-middleware


This is the python middleware for gx4ki.
It consists currently of 2 modules, which is responsible for the execution of specific jobs in a in a k8s environment.

## Middleware k8s_client

The first module (main.py) implements a k8s_client and manges k8s resources in a backend cluster.
The module is able to create and delete Pods and ConfigMaps.
A Job is a User-specific workload which can currently defined by an environment file, which holds information of the used container image (WORKER_IMAGE) of the job, the information if a GPU (GPU) is required and the information where the result is stored (RESULT_DIRECTORY)
Additional runtime information for the worker image can be provided by environment files, from which configmaps are created and tied to to job-job as a env-ref.

The k8s_client is currently a prototype for deploying a user-specific job defined by it's job-config and a optional runtime configurations.
At the moment, the processing a job is very strict implemented and processed in a single call (demo-call).
During that singel call it ...:
1. creates optional config maps from .env files
2. creates pod manifests
3. deploys the pod
4. waits for the termination of the WORKER_IMAGE
5. stores result files to S3 Storage


## Middleware entrypoint

The second module is the middleware_entrypoint (entrypoint.py) which is for the moment of time a simple interface to trigger a specific job in the k8s_client.
Depending on the call it reads the job description and the job related environment file, located at [resources](./resources/), and builds the request for the k8s_client.
In addition, this module implements the provision functionally of an EDC-Connector, therefore it can be used as an provisioner-backend from such an connector.


## API

For testing purpose ever function against the k8s-cluster has its on minimal api endpoint.
To test the individual endpoints use the [api-collection (thundercliend)](./test/resources/thunder-collection_imla-k8s-api.json)

---
## Demo Deployment


### Build Middleware Container

The final container Contains k8s_client and mw_entrypoint module

```shell
docker build --no-cache --pull -t  harbor.gx4ki.imla.hs-offenburg.de/gx4ki/imla-k8s-client:latest .
```

### Deployment


**Deploy k8s_client**

To run the the k8s_client in the Kubernetes some preconditions are required.
1. Apply the service Account: `kubectl apply -f k8s/gx4ki-sa-auth.yaml`
2. Create a middleware config `cp config/middlelayer.conf.tmpl config/middlelayer.conf`
   - provide the required configurations options
   - and apply the secret
3. if necessary create and apply an registry-secret
4. deploy the k8s client into the cluster or run it as a standalone container (requries a kubeconfig file)


```shell
docker run -it --rm --name k8s-client \
-v $(pwd)/config/minikube.kubeconfig:/opt/k8s-api/config/kubeconfig \
-v $(pwd)/config/middlelayer.conf:/opt/k8s-api/config/middlelayer.conf \
--network minikube \
--ip 192.168.49.5 \
gx4ki/imla-k8s-api
```

**Deploy Middleware Entrypoint**

Deploy a standalone mw_entrypoint with the provided docker-compose file.

```shell
docker-compose -f docker-compose.entrypoint.yaml up
```

### Work Items

- **DONE** create ServiceAccount which deploys jobs into a specifc namespace
- **DONE** create ConfigMaps from env-file and use it in jobs
- **POSTPONED**how to get dotenv-file from outside to imla-api? [comunication-overview](./docs/edc-dotenv-transfer.excalidraw)
  - dotenv-file and job-config files read from local directory
- **DONE** adapt job to carla
- **FAIL** download result files directly to imla-k8s-client -- rosbag file changed during transfer()
- **DONE** upload result files to minio
  - **DONE** get resultfile from minio and send it as response
- **DONE** deploy k8s-api in gx4ki-cluster
- test with connector
  - **FAIL** timeout problems after trigger carle and wait for result.
    - there is a provider provision tate in the in the workflow of the transfer.
      This state works with webhooks and callbacks which enables to process a long running job, bevor the data is ready.
      This functionality facilitates the gx4ki-middlelayer to deploy a long running job into.
      After the job finishes the callback communicates to the connector where the result data can be fetched.
  - **DONE** Implement provision handler in entrypoint


### Demo Use-Case

Carla-Demo V0.1

Run all in once:
1. trigger job in provison phase of connector

2. trigger job execution with job-config and dotenv-file
    - create configmap from dotenv-file
    - deploy job in cluster
    - wait for finish
    - store rosbag result

3. post callback to connector with storage information
4. data transfer from minio over connector to datasink

![middlelayer-overview](./docs/middlelayer-overview.png)


### Discussion

**Provision Compute**

I started a [Discussion#2405](https://github.com/eclipse-edc/Connector/discussions/2405) in EDC-Connector Repo, how we can address the feature of compute-/ service-offering in a data space.
As far as I understand the answer, which I got so far. The connector is just used to provision a backend data-plane and some secrets to access it.
The following picture shows that concept.

![concept-edc-service-provider](./docs/edc-service-provider.png)



---

## Developer Topics

Some useful information during development.

### Create ServiceAccount

To be authorized against the control plane of the k8s api a serviceaccount is created and used.
This serviceaccount is bound to its `clusterrolebindung` to a `clusterrole` with the allowed rules/permissions.

- a `serviceAccount` is only from within the cluster usable.
- a cluster wide RBAC is used becaus at api start the `/healthz` of k8s is requested
  - this should be replaced

https://kubernetes.io/docs/reference/access-authn-authz/rbac/
https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/
https://betterprogramming.pub/k8s-tips-using-a-serviceaccount-801c433d0023
https://stackoverflow.com/questions/71265398/kuberentes-rbac-rule-to-allow-creating-jobs-only-from-a-cronjob

### Create ConfigMaps in K8s

Applications in Pods are requred to be configurable by environment variables.
Environment and other configuraion files can be stored in k8s by ConfigMaps and/or Secrets.

### Read Result Data from Pods

Read/Load Files from a Container is very complicated to achieve.

what have I tried so far:
- use kubernetes-python client and stream the data via stdout out of the container by executing `cat resultfile`
  - rosbag file (binary file) is somehow changed by transfer
- create kubernetes-python client equivalent to `kubectl cp` (-> internally it's a kube exec with tar cf | tar xf )
