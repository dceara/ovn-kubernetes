ovnk1=$(kubectl get pod -n ovn-kubernetes -o wide | grep "ovn-worker " | grep ovnkube | awk '{print $1}')
ovnk2=$(kubectl get pod -n ovn-kubernetes -o wide | grep "ovn-worker2 " | grep ovnkube | awk '{print $1}')

### L2 vars ###

l2_client=$(kubectl get pod -n evpn-demo -o wide | grep "ovn-worker " | awk '{print $1}')
l2_server=$(kubectl get pod -n evpn-demo -o wide | grep "ovn-worker2 " | awk '{print $1}')
l2_server_ip=$(kubectl get pod -n evpn-demo $l2_server -o json | jq -r '.metadata.annotations["k8s.v1.cni.cncf.io/network-status"] | fromjson | .[] | select(.default == true) | .ips[0]')

### L3 vars ###

l3_client=$(kubectl get pod -n evpn-l3-demo -o wide | grep "ovn-worker " | awk '{print $1}')
l3_server=$(kubectl get pod -n evpn-l3-demo -o wide | grep "ovn-worker2 " | awk '{print $1}')
l3_client_ip=$(kubectl get pod -n evpn-l3-demo $l3_server -o json | jq -r '.metadata.annotations["k8s.v1.cni.cncf.io/network-status"] | fromjson | .[] | select(.default == true) | .ips[0]')
l3_server_ip=$(kubectl get pod -n evpn-l3-demo $l3_server -o json | jq -r '.metadata.annotations["k8s.v1.cni.cncf.io/network-status"] | fromjson | .[] | select(.default == true) | .ips[0]')

l3_new_client=$(kubectl get pod -n evpn-l3-new -o wide | grep "ovn-worker " | awk '{print $1}')
l3_new_server=$(kubectl get pod -n evpn-l3-new -o wide | grep "ovn-worker2 " | awk '{print $1}')
l3_new_client_ip=$(kubectl get pod -n evpn-l3-new $l3_new_server -o json | jq -r '.metadata.annotations["k8s.v1.cni.cncf.io/network-status"] | fromjson | .[] | select(.default == true) | .ips[0]')
l3_new_server_ip=$(kubectl get pod -n evpn-l3-new $l3_new_server -o json | jq -r '.metadata.annotations["k8s.v1.cni.cncf.io/network-status"] | fromjson | .[] | select(.default == true) | .ips[0]')
