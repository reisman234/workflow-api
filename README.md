# gx4ki-k8s-api


API to schedule gx4ki-jobs in a k8s cluster.
At the moment, this API is just a prototyp for creating static jobs related to gx4ki minimal carla use-case.

The API is running as standalone FastApi Project and uses kubernets-client/python module to talk with an k8s.
To be able to talk with the cluster a kubeconfig is required.
Minikube is used as a k8s development backend locally.
VSCode's Remote Container Dev is used to develop the api in the same network as the Cluster with the an static IP

---
To Start the API:

```shell
uvicorn main:app --reload  --host 0.0.0.0 --port 8888
```


Interact with the API:

```shell
# create static a job, returns a job_id
JOB_ID=$(curl -XGET 192.168.49.5:8888/job/deploy | jq -r .job_uuid)

curl -XDELETE 192.168.49.5:8888/job/$JOB_ID

# list files in result directory
curl -XGET 192.168.49.5:8888/job/$JOB_ID/result/list

# request a file from the result directory
curl -XGET 192.168.49.5:8888/job/$JOB_ID/result?file=<FILENAME>
```


---
## DEMO

```
                                              |--------------|
                                              |   k8s        |
--------------       -------------------      -------------  |
|  consumer  |  <->  |  imla-provider  |  <-> |  k8s-api  |  |
--------------       -------------------      ---------------|
```

Running a static prototype demo in the api.
This call creates and deploys a job on k8s, and blocks until the worker container finishes.
After that it reads a spesific result file from that job via a side-car container.

```shell
curl -XGET 192.168.49.5:8888/demo > resultfile
```

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

**Open Questions**


### Read Result Data from Pods

Read/Load Files from a Container is very complicated to achieve.

what have I tried so far:
- use kubernetes-python client and stream the data via stdout out of the container by executing `cat resultfile`
  - rosbag file (binary file) is somehow changed by transfer
- create kubernetes-python client equivalent to `kubectl cp` (-> internally it's a kube exec with tar cf | tar xf )



# Bulild & Run API as Container

```shell
docker build --no-cache --pull -t gx4ki/imla-k8s-client:latest .
docker tag gx4ki/imla-k8s-client:latest harbor.gx4ki.imla.hs-offenburg.de/gx4ki/imla-k8s-client:latest
```


```shell
docker run -it --rm --name k8s-client \
-v $(pwd)/config/minikube.kubeconfig:/opt/k8s-api/config/kubeconfig \
-v $(pwd)/config/middlelayer.conf:/opt/k8s-api/config/middlelayer.conf \
--network minikube \
--ip 192.168.49.5 \
gx4ki/imla-k8s-api
```

**Deploy a proxy Endpoint for connector**

Deploy a proxy Endpoint for connector

```shell
docker-compose -f docker-compose.entrypoint.yaml up
```


---
**TODO**

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
