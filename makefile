
default:
	echo "invalid target"

demo-config:
	cp config/kube/config.tmpl config/kube/config
	kubectl create namespace gx4ki-demo
	kubectl config set contexts.minikube.namespace gx4ki-demo
	kubectl apply -f k8s/gx4ki-sa-auth.yaml

	kubectl cluster-info | grep "control plane" | grep -oP '(?<=https://).*?(?=:8443)' | xargs -i sed -i "s/{MINIKUBE_IP}/{}/" config/kube/config
	kubectl get secrets workflow-api-token -o json | jq -r '.data."ca.crt"' | xargs -i sed -i "s/{MINIKUBE_CA_CERT}/{}/" config/kube/config
	kubectl get secrets workflow-api-token -o json | jq -r .data.token | base64 -d | xargs -i sed -i "s/{GX4KI_API_SERVICE_ACCOUNT_TOKEN}/{}/" config/kube/config
