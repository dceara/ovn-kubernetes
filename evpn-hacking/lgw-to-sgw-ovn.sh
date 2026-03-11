#!/bin/bash

set -ex

function get_ovnk_pod()
{
    local node_name=$1
    kubectl get pod -n ovn-kubernetes -o wide | grep "$node_name " | grep ovnkube | awk '{print $1}'
}

function get_node_ip()
{
    local node_name=$1
    kubectl get node -o wide | grep "$node_name " | awk '{print $6}'
}

function create_config()
{
    kubectl create -f cudn-config.yaml
}

function delete_config()
{
    set +e

    kubectl delete -f cudn-config.yaml

    # Cleanup manually added L3 EVPN resources.
    for node in ovn-worker ovn-worker2; do
        local ovnk=$(get_ovnk_pod $node)
        local evpn_ls=evpn_ls_cluster_udn_evpn.l3_$node
        kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl lrp-del lrp-evpn
        kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl ls-del $evpn_ls

        local evpn_ls=evpn_ls_cluster_udn_evpn.l3.new_$node
        kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl lrp-del lrp-evpn-new
        kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl ls-del $evpn_ls
    done
}

trap "delete_config" EXIT

create_config

echo Wait for stuff to be configured in ovnk.
sleep 10

. ./vars.sh

#############################################
#         Generic EVPN OVN setup            #
#############################################
for node in ovn-worker ovn-worker2; do
    node_ip=$(get_node_ip $node)
    ovnk=$(get_ovnk_pod $node)

    kubectl exec -n ovn-kubernetes $ovnk -- ovs-vsctl set open . external-ids:ovn-evpn-local-ip=$node_ip
    kubectl exec -n ovn-kubernetes $ovnk -- ovs-vsctl set open . external-ids:ovn-evpn-vxlan-ports=4789
done

echo Wait for ovn-controller to catch up
sleep 2

#############################################
#            L2 UDN / MAC VRF               #
#############################################

# VNI corresponding to the L2 UDN configured in cudn-config.yaml.
vni=20100

for node in ovn-worker ovn-worker2; do
    node_ip=$(get_node_ip $node)
    ovnk=$(get_ovnk_pod $node)

    # Delete EVPN ovnk LGW port towards host, we don't need it in SGW for forwarding
    # traffic, we do keep the host side of the port in order to install static entries
    # for the FDB records we want to advertise.
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl lsp-del macvrf-cluster_udn_evpn.l2_ovn_layer2_switch

    # Enable OVN native EVPN support on the L2 UDN switch.
    # We need to set:
    # - VNI to use for that logical switch, i.e., $vni
    # - host side evpn bridge interface to monitor, i.e., svl2-${l2_udn_name}
    # - host side vxlan interface (used by FRR to learn/advertise VTEPs), i.e., evx4-${vtep-name}
    # - host side SVI interface (uses by OVN to advertise local MACs/IPs), i.e., evpn-${l2_udn_name}
    evpn_ls=$(kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl --bare --columns name list logical_switch | grep '^cluster_udn_evpn\.l2')
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_switch $evpn_ls other_config:dynamic-routing-vni=$vni
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_switch $evpn_ls other_config:dynamic-routing-bridge-ifname=svl2-evpn-l2
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_switch $evpn_ls other_config:dynamic-routing-vxlan-ifname=evx4-evpn-vtep
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_switch $evpn_ls other_config:dynamic-routing-advertise-ifname=evpn-evpn-l2

    # Enable OVN native announcement of known MACs and IPs on the L2 UDN switch.
    # TODO: once https://redhat.atlassian.net/browse/FDP-2742 is implemented
    #       ovn-kubernetes should opt out form advertising the following:
    #       - L2 UDN mgmt port MAC + IP
    #       - L2 UDN transit router port MAC + IP
    #       these are local to each node
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_switch $evpn_ls other_config:dynamic-routing-redistribute=fdb,ip

done

# Check connectivity:
# Traffic to remote pods should go through VXLAN, fully through OVN's br-int
# and not through the host!
kubectl exec -it -n evpn-demo $l2_client -- ping -c1 $l2_server_ip


#############################################
#            L3 UDN / IP VRF               #
#############################################

# VNI corresponding to the L3 UDN configured in cudn-config.yaml.
vni=20101
for node in ovn-worker ovn-worker2; do
    ovnk=$(get_ovnk_pod $node)
    vrf=$(kubectl exec -n ovn-kubernetes $ovnk -- ip vrf | grep 'evpn-l3 ' | awk '{print $2}')
    evpn_router=GR_cluster_udn_evpn.l3_$node
    mac=$(kubectl exec -n ovn-kubernetes $ovnk -- ip link show dev svl3-evpn-l3 | grep ether | awk '{print $2}')

    # TODO: fake IP for the new router port
    fake_ip=42.42.42.42/32

    # Enable OVN native EVPN support on the a new logical switch in the L3 UDN.
    # - That's where we stitch the IP VRFs together
    # We need to set:
    # - VNI to use for that logical switch, i.e., $vni
    # - host side evpn bridge interface to monitor, i.e., svl3-${l3_udn_name}
    # - host side vxlan interface (used by FRR to learn/advertise VTEPs), i.e.,
    #   evx4-${vtep-name}
    # - host side SVI interface (used by OVN to advertise local MACs/IPs): we
    #   set it to an inexistent interface for now; we're only interested in
    #   learning remote VTEPs and how they're accessible, we don't need to
    #   advertise any workloads here, that's done on the cluster router.
    evpn_ls=evpn_ls_cluster_udn_evpn.l3_$node
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl lrp-add $evpn_router lrp-evpn $mac $fake_ip
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl ls-add $evpn_ls
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl lsp-add-router-port $evpn_ls lsp-evpn-lr lrp-evpn
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_switch $evpn_ls other_config:dynamic-routing-vni=$vni
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_switch $evpn_ls other_config:dynamic-routing-bridge-ifname=svl3-evpn-l3
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_switch $evpn_ls other_config:dynamic-routing-vxlan-ifname=evx4-evpn-vtep
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_switch $evpn_ls other_config:dynamic-routing-advertise-ifname=foobar

    # Enable OVN native BGP route learning on the gateway router:
    # - Routes to reach remote workloads (through EVPN) will be learned here.
    # - Traffic destined to a remote part of the current IP-VRF leaves the
    #   node through the GR (including host -> UDN nodePort service traffic)
    # - We need to set:
    #   - dynamic-routing=true (route learning)
    #   - monitor $vrf for dynamically learned routes (BGP routes installed by
    #     FRR)
    #   - disable dynamic routing on rtoj-GR_cluster_udn_evpn.l3_$node and
    #     rtoe-GR_cluster_udn_evpn.l3_$node, we only
    #     want to learn routes via the lrp-evpn port
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_router $evpn_router options:dynamic-routing=true
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_router $evpn_router options:dynamic-routing-vrf-id=$vrf
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_router_port rtoj-GR_cluster_udn_evpn.l3_$node options:dynamic-routing-no-learning=true
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_router_port rtoe-GR_cluster_udn_evpn.l3_$node options:dynamic-routing-no-learning=true

    # Enable OVN native BGP route advertising (NO LEARNING) on the cluster router:
    # - Routes to reach the L3 UDN connected network will be advertised here.
    # - We need to set:
    #   - dynamic-routing=true
    #   - dynamic-routing-no-learning=true (we only advertise on this router)
    #   - monitor $vrf (same VRF as for the GR) to reconcile routes we advertise
    # TODO: once https://redhat.atlassian.net/browse/FDP-2742 is implemented
    #       ovn-kubernetes should opt out form advertising the following:
    #       - connected join subnet
    evpn_advertise_router=cluster_udn_evpn.l3_ovn_cluster_router
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_router $evpn_advertise_router options:dynamic-routing=true
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_router $evpn_advertise_router options:dynamic-routing-vrf-id=$vrf
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_router $evpn_advertise_router options:dynamic-routing-no-learning=true
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_router $evpn_advertise_router options:dynamic-routing-redistribute=connected
done

# Check connectivity:
# Traffic to remote pods should go through VXLAN, fully through OVN's br-int
# and not through the host!
kubectl exec -it -n evpn-l3-demo $l3_client -- ping -c1 $l3_server_ip

# VNI corresponding to the L3 UDN configured in cudn-config.yaml.
vni=20202
for node in ovn-worker ovn-worker2; do
    ovnk=$(get_ovnk_pod $node)
    vrf=$(kubectl exec -n ovn-kubernetes $ovnk -- ip vrf | grep 'evpn-l3-new ' | awk '{print $2}')
    evpn_router=GR_cluster_udn_evpn.l3.new_$node
    ifname=$(kubectl exec -n ovn-kubernetes $ovnk -- ip link show | grep svl3- | grep evbr-evpn-vtep | grep -v svl3-evpn | cut -f 2 -d ' ' | cut -f 1 -d '@')
    mac=$(kubectl exec -n ovn-kubernetes $ovnk -- ip link show dev $ifname | grep ether | awk '{print $2}')

    # TODO: fake IP for the new router port
    fake_ip=42.42.42.42/32

    # Enable OVN native EVPN support on the a new logical switch in the L3 UDN.
    # - That's where we stitch the IP VRFs together
    # We need to set:
    # - VNI to use for that logical switch, i.e., $vni
    # - host side evpn bridge interface to monitor, i.e., svl3-${l3_udn_name}
    # - host side vxlan interface (used by FRR to learn/advertise VTEPs), i.e.,
    #   evx4-${vtep-name}
    # - host side SVI interface (used by OVN to advertise local MACs/IPs): we
    #   set it to an inexistent interface for now; we're only interested in
    #   learning remote VTEPs and how they're accessible, we don't need to
    #   advertise any workloads here, that's done on the cluster router.
    evpn_ls=evpn_ls_cluster_udn_evpn.l3.new_$node
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl lrp-add $evpn_router lrp-evpn-new $mac $fake_ip
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl ls-add $evpn_ls
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl lsp-add-router-port $evpn_ls lsp-evpn-lr-new lrp-evpn-new
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_switch $evpn_ls other_config:dynamic-routing-vni=$vni
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_switch $evpn_ls other_config:dynamic-routing-bridge-ifname=$ifname
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_switch $evpn_ls other_config:dynamic-routing-vxlan-ifname=evx4-evpn-vtep
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_switch $evpn_ls other_config:dynamic-routing-advertise-ifname=foobar

    # Enable OVN native BGP route learning on the gateway router:
    # - Routes to reach remote workloads (through EVPN) will be learned here.
    # - Traffic destined to a remote part of the current IP-VRF leaves the
    #   node through the GR (including host -> UDN nodePort service traffic)
    # - We need to set:
    #   - dynamic-routing=true (route learning)
    #   - monitor $vrf for dynamically learned routes (BGP routes installed by
    #     FRR)
    #   - disable dynamic routing on rtoj-GR_cluster_udn_evpn.l3_$node and
    #     rtoe-GR_cluster_udn_evpn.l3_$node, we only
    #     want to learn routes via the lrp-evpn port
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_router $evpn_router options:dynamic-routing=true
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_router $evpn_router options:dynamic-routing-vrf-id=$vrf
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_router_port rtoj-GR_cluster_udn_evpn.l3.new_$node options:dynamic-routing-no-learning=true
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_router_port rtoe-GR_cluster_udn_evpn.l3.new_$node options:dynamic-routing-no-learning=true

    # Enable OVN native BGP route advertising (NO LEARNING) on the cluster router:
    # - Routes to reach the L3 UDN connected network will be advertised here.
    # - We need to set:
    #   - dynamic-routing=true
    #   - dynamic-routing-no-learning=true (we only advertise on this router)
    #   - monitor $vrf (same VRF as for the GR) to reconcile routes we advertise
    # TODO: once https://redhat.atlassian.net/browse/FDP-2742 is implemented
    #       ovn-kubernetes should opt out form advertising the following:
    #       - connected join subnet
    # evpn_advertise_router=cluster_udn_evpn.l3.new_ovn_cluster_router
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_router $evpn_advertise_router options:dynamic-routing=true
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_router $evpn_advertise_router options:dynamic-routing-vrf-id=$vrf
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_router $evpn_advertise_router options:dynamic-routing-no-learning=true
    kubectl exec -n ovn-kubernetes $ovnk -- ovn-nbctl set logical_router $evpn_advertise_router options:dynamic-routing-redistribute=connected
done

# Check connectivity:
# Traffic to remote pods should go through VXLAN, fully through OVN's br-int
# and not through the host!
kubectl exec -it -n evpn-l3-new $l3_new_client -- ping -c1 $l3_new_server_ip

sleep infinity
