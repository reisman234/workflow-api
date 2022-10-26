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


Running a static prototype demo in the api.
This call creates and deploys a job on k8s, and blocks until the worker container finishes.
After that it reads a spesific result file from that job via a side-car container.

```shell
curl -XGET 192.168.49.5:8888/demo > resultfile
```