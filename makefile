
NAMESPACE=workflow-api-demo

default:
	echo "invalid target"

delete-demo-config:
	kubectl delete namespace ${NAMESPACE}

demo-config:
	cp config/kube/config.tmpl config/kube/config

	kubectl create namespace ${NAMESPACE}
	kubectl -n ${NAMESPACE} apply -f k8s/gx4ki-sa-auth.yaml

	kubectl cluster-info | grep "control plane" | grep -oP '(?=https://).*(?<=:(6|8)443)' | xargs -i sed -i "s|{MINIKUBE_IP}|{}|" config/kube/config
	kubectl -n ${NAMESPACE} get secrets workflow-api-token -o json | jq -r '.data."ca.crt"' | xargs -i sed -i "s/{MINIKUBE_CA_CERT}/{}/" config/kube/config
	kubectl -n ${NAMESPACE} get secrets workflow-api-token -o json | jq -r .data.token | base64 -d | xargs -i sed -i "s/{API_SERVICE_ACCOUNT_TOKEN}/{}/" config/kube/config

	kubectl -n ${NAMESPACE} apply -f k8s/secrets/imla-registry-secret.yaml
	kubectl -n ${NAMESPACE} create secret generic workflow-api-config --from-file config/workflow-api.cfg
