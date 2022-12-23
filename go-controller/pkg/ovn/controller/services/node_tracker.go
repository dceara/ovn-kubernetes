package services

import (
	"net"
	"reflect"
	"sort"
	"sync"

	globalconfig "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	v1 "k8s.io/api/core/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// nodeTracker watches all Node objects and maintains a cache of information relevant
// to service creation. If a new node is created, it requests a resync of all services,
// since need to apply those service's load balancers to the new node as well.
type nodeTracker struct {
	sync.Mutex

	// nodes is the list of nodes we know about
	// map of name -> info
	nodes map[string]nodeInfo

	// resyncFn is the function to call so that all service are resynced
	resyncFn func(nodeIPv4Template, nodeIPv6Template *Template)

	// Template variables expanding to each chassis' node IP (v4 and v6).
	nodeIPv4Template *Template
	nodeIPv6Template *Template
}

type nodeInfo struct {
	// the node's Name
	name string
	// The list of physical IPs the node has, as reported by the gatewayconf annotation
	nodeIPs []net.IP
	// The pod network subnet(s)
	podSubnets []net.IPNet
	// the name of the node's GatewayRouter, or "" of non-existent
	gatewayRouterName string
	// The name of the node's switch - never empty
	switchName string
	// The chassisID of the node (ovs.external-ids:system-id)
	chassisID string
}

func (ni *nodeInfo) nodeIPsStr() []string {
	out := make([]string, 0, len(ni.nodeIPs))
	for _, nodeIP := range ni.nodeIPs {
		out = append(out, nodeIP.String())
	}
	return out
}

// returns a list of all ip blocks "assigned" to this node
// includes node IPs, still as a mask-1 net
func (ni *nodeInfo) nodeSubnets() []net.IPNet {
	out := append([]net.IPNet{}, ni.podSubnets...)
	for _, ip := range ni.nodeIPs {
		if ipv4 := ip.To4(); ipv4 != nil {
			out = append(out, net.IPNet{
				IP:   ip,
				Mask: net.CIDRMask(32, 32),
			})
		} else {
			out = append(out, net.IPNet{
				IP:   ip,
				Mask: net.CIDRMask(128, 128),
			})
		}
	}

	return out
}

func newNodeTracker(nodeInformer coreinformers.NodeInformer) (*nodeTracker, error) {
	nt := &nodeTracker{
		nodes:            map[string]nodeInfo{},
		nodeIPv4Template: makeTemplate(makeLBNodeIPTemplateName(v1.IPv4Protocol)),
		nodeIPv6Template: makeTemplate(makeLBNodeIPTemplateName(v1.IPv6Protocol)),
	}

	_, err := nodeInformer.Informer().AddEventHandler(factory.WithUpdateHandlingForObjReplace(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			node, ok := obj.(*v1.Node)
			if !ok {
				return
			}
			nt.updateNode(node)
		},
		UpdateFunc: func(old, new interface{}) {
			oldObj, ok := old.(*v1.Node)
			if !ok {
				return
			}
			newObj, ok := new.(*v1.Node)
			if !ok {
				return
			}
			// Make sure object was actually changed and not pending deletion
			if oldObj.GetResourceVersion() == newObj.GetResourceVersion() || !newObj.GetDeletionTimestamp().IsZero() {
				return
			}

			// updateNode needs to be called only when hostSubnet annotation has changed or
			// if L3Gateway annotation's ip addresses have changed or the name of the node (very rare)
			// has changed. No need to trigger update for any other field change.
			if util.NodeSubnetAnnotationChanged(oldObj, newObj) || util.NodeL3GatewayAnnotationChanged(oldObj, newObj) ||
				util.NodeChassisIDAnnotationChanged(oldObj, newObj) || oldObj.Name != newObj.Name {
				nt.updateNode(newObj)
			}
		},
		DeleteFunc: func(obj interface{}) {
			node, ok := obj.(*v1.Node)
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					klog.Errorf("Couldn't understand non-tombstone object")
					return
				}
				node, ok = tombstone.Obj.(*v1.Node)
				if !ok {
					klog.Errorf("Couldn't understand tombstone object")
					return
				}
			}
			nt.removeNode(node.Name, true)
		},
	}))
	if err != nil {
		return nil, err
	}
	return nt, nil

}

// updateNodeInfo updates the node info cache, and syncs all services
// if it changed.
func (nt *nodeTracker) updateNodeInfo(nodeName, switchName, routerName, chassisID string, nodeIPs []net.IP, podSubnets []*net.IPNet) {
	ni := nodeInfo{
		name:              nodeName,
		nodeIPs:           nodeIPs,
		podSubnets:        make([]net.IPNet, 0, len(podSubnets)),
		gatewayRouterName: routerName,
		switchName:        switchName,
		chassisID:         chassisID,
	}
	for i := range podSubnets {
		ni.podSubnets = append(ni.podSubnets, *podSubnets[i]) // de-pointer
	}

	nt.Lock()
	if existing, ok := nt.nodes[nodeName]; ok {
		if reflect.DeepEqual(existing, ni) {
			nt.Unlock()
			return
		}
	}

	nt.nodes[nodeName] = ni
	if chassisID != "" {
		// Services are currently supported only on the node's first IP.
		// Extract that one and populate the node's IP template value.
		if globalconfig.IPv4Mode {
			if ipv4, err := util.MatchFirstIPFamily(false, nodeIPs); err == nil {
				nt.nodeIPv4Template.Value[chassisID] = ipv4.String()
			}
		}
		if globalconfig.IPv6Mode {
			if ipv6, err := util.MatchFirstIPFamily(true, nodeIPs); err == nil {
				nt.nodeIPv6Template.Value[chassisID] = ipv6.String()
			}
		}
	}
	nodeIPv4Template, nodeIPv6Template := *nt.nodeIPv4Template, *nt.nodeIPv6Template
	nt.Unlock()

	klog.Infof("Node %s switch + router changed, syncing services", nodeName)
	// Resync all services
	nt.resyncFn(&nodeIPv4Template, &nodeIPv6Template)
}

// RemoveNode removes a node from the LB -> node mapper
// If 'doResync' is true then this also forces a full reconciliation of
// services.
func (nt *nodeTracker) removeNode(nodeName string, doResync bool) {
	nt.Lock()

	if node, found := nt.nodes[nodeName]; found {
		delete(nt.nodeIPv4Template.Value, node.chassisID)
		delete(nt.nodeIPv6Template.Value, node.chassisID)
	}
	delete(nt.nodes, nodeName)
	nodeIPv4Template, nodeIPv6Template := *nt.nodeIPv4Template, *nt.nodeIPv6Template
	nt.Unlock()

	if doResync {
		nt.resyncFn(&nodeIPv4Template, &nodeIPv6Template)
	}
}

// UpdateNode is called when a node's gateway router / switch / IPs have changed
// The switch exists when the HostSubnet annotation is set.
// The gateway router will exist sometime after the L3Gateway annotation is set.
func (nt *nodeTracker) updateNode(node *v1.Node) {
	klog.V(2).Infof("Processing possible switch / router updates for node %s", node.Name)
	hsn, err := util.ParseNodeHostSubnetAnnotation(node, types.DefaultNetworkName)
	if err != nil || hsn == nil {
		// usually normal; means the node's gateway hasn't been initialized yet
		klog.Infof("Node %s has invalid / no HostSubnet annotations (probably waiting on initialization): %v", node.Name, err)
		nt.removeNode(node.Name, false)
		return
	}

	switchName := node.Name
	grName := ""
	ips := []net.IP{}
	chassisID := ""

	// if the node has a gateway config, it will soon have a gateway router
	// so, set the router name
	gwConf, err := util.ParseNodeL3GatewayAnnotation(node)
	if err != nil || gwConf == nil {
		klog.Infof("Node %s has invalid / no gateway config: %v", node.Name, err)
	} else if gwConf.Mode != globalconfig.GatewayModeDisabled {
		grName = util.GetGatewayRouterFromNode(node.Name)
		if gwConf.NodePortEnable {
			for _, ip := range gwConf.IPAddresses {
				ips = append(ips, ip.IP)
			}
		}
		chassisID = gwConf.ChassisID
	}

	nt.updateNodeInfo(
		node.Name,
		switchName,
		grName,
		chassisID,
		ips,
		hsn,
	)
}

// allNodes returns a list of all nodes (and their relevant information) along
// with the node's IPv4 and IPv6 templates.
func (nt *nodeTracker) allNodes() ([]nodeInfo, Template, Template) {
	nt.Lock()
	defer nt.Unlock()

	out := make([]nodeInfo, 0, len(nt.nodes))
	for _, node := range nt.nodes {
		out = append(out, node)
	}

	// Sort the returned list of nodes
	// so that other operations that consume this data can just do a DeepEquals of things
	// (e.g. LB routers + switches) without having to do set arithmetic
	sort.SliceStable(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out, *nt.nodeIPv4Template, *nt.nodeIPv6Template
}
