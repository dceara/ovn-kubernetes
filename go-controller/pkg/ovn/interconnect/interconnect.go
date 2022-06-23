package interconnect

import (
	"context"
	"fmt"
	"math/big"
	"net"
	"reflect"
	"strconv"
	"sync"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"

	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/sbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	utilnet "k8s.io/utils/net"
)

const (
	transitSwitchTunnelKey = "16711683"
)

type nodeInfo struct {
	// the node's Name
	name string

	// Is the node part of global availability zone.
	isNodeGlobalAz bool

	tsMac net.HardwareAddr
	tsNet string
	tsIp  string
	id    int

	nodeSubnets   []*net.IPNet
	joinSubnets   []*net.IPNet
	nodeGRIPs     []*net.IPNet
	chassisID     string
	nodePrimaryIp string
}

type Controller struct {
	sync.Mutex

	client clientset.Interface
	kube   kube.Interface

	// libovsdb northbound client interface
	nbClient libovsdbclient.Client

	// libovsdb southbound client interface
	sbClient libovsdbclient.Client

	sharedInformer informers.SharedInformerFactory

	localAzName string

	// nodes is the list of nodes we know about
	// map of name -> info
	nodes map[string]nodeInfo

	localNodeInfo *nodeInfo

	tsBaseIp *big.Int
}

// NewInterconnect creates a new interconnect
func NewController(client clientset.Interface, kube kube.Interface, nbClient libovsdbclient.Client,
	sbClient libovsdbclient.Client) *Controller {
	sharedInformer := informers.NewSharedInformerFactory(client, 0)
	nodeInformer := sharedInformer.Core().V1().Nodes().Informer()
	icConnect := Controller{
		client:         client,
		kube:           kube,
		nbClient:       nbClient,
		sbClient:       sbClient,
		sharedInformer: sharedInformer,
		nodes:          map[string]nodeInfo{},
		localAzName:    "",
		localNodeInfo:  nil,
		tsBaseIp:       utilnet.BigForIP(net.IPv4(169, 254, 0, 0)),
	}

	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			node, ok := obj.(*v1.Node)
			klog.Infof("Interconnect: AddFunc for : %s", node.Name)
			if !ok {
				return
			}
			icConnect.updateNode(node)
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
			klog.Infof("Interconnect: UpdateFunc for : %s", newObj.Name)

			// Make sure object was actually changed and not pending deletion
			if oldObj.GetResourceVersion() == newObj.GetResourceVersion() || !newObj.GetDeletionTimestamp().IsZero() {
				klog.Infof("Interconnect: UpdateFunc for : %s returning.. nothing changed", newObj.Name)
				return
			}

			icConnect.updateNode(newObj)
		},
		DeleteFunc: func(obj interface{}) {
			node, ok := obj.(*v1.Node)
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					klog.Errorf("couldn't understand non-tombstone object")
					return
				}
				node, ok = tombstone.Obj.(*v1.Node)
				if !ok {
					klog.Errorf("couldn't understand tombstone object")
					return
				}
			}
			klog.Infof("Interconnect: DeleteFunc for : %s", node.Name)
			icConnect.removeNode(node.Name)
		},
	})

	return &icConnect
}

func (ic *Controller) Run(stopCh <-chan struct{}) error {
	ic.updateLocalAzName()
	ic.sharedInformer.Start(stopCh)
	<-stopCh
	return nil
}

func (ic *Controller) updateLocalAzName() {
	ic.Lock()
	defer ic.Unlock()
	localAzName, err := libovsdbops.GetNBAvailbilityZoneName(ic.nbClient)
	if err == nil {
		ic.localAzName = localAzName
	}
}

func (ic *Controller) updateNode(node *v1.Node) {
	nodeId := util.GetNodeId(node)
	if nodeId == -1 {
		// Don't consider this node as master has not allocated node id yet.
		return
	}

	if ic.localAzName == "" {
		ic.updateLocalAzName()
	}

	ic.updateNodeInfo(node, nodeId)
	if ni, ok := ic.nodes[node.Name]; ok {
		err := ic.syncNodeChassis(ni)
		if err != nil {
			klog.Errorf("Failed to sync the node chassis for node %s -  %v", node.Name, err)
		}
		ic.syncNodeResources(ni)
	}
}

func (ic *Controller) removeNode(nodeName string) {
	if ni, ok := ic.nodes[nodeName]; ok {
		ic.Lock()
		defer ic.Unlock()
		_ = libovsdbops.DeleteNodeChassis(ic.sbClient, ni.name)
		//ic.cleanupNodeResources(ni)
		delete(ic.nodes, nodeName)
	}
}

func (ic *Controller) updateNodeInfo(node *v1.Node, nodeId int) {
	//TODO: generate mac address and IP properly.  Its a hack for now.
	tsIp := utilnet.AddIPOffset(ic.tsBaseIp, nodeId)
	tsMac := util.IPAddrToHWAddr(tsIp)

	chassisID, err := util.ParseNodeChassisIDAnnotation(node)
	if err != nil {
		klog.Errorf("Failed to parse node chassis-id for node - %s", node.Name)
		return
	}

	nodeSubnets, err := util.ParseNodeHostSubnetAnnotation(node)
	if err != nil {
		klog.Errorf("Failed to parse node %s subnets annotation %v", node.Name, err)
		return
	}

	nodePrimaryIp, err := util.GetNodePrimaryIP(node)
	if err != nil {
		klog.Errorf("Failed to parse node %s primary IP %v", node.Name, err)
		return
	}

	nodeGRIPs, err := util.ParseNodeGRIPsAnnotation(node)
	if err != nil {
		klog.Infof("Failed to parse node %s GR IPs annotation %v", node.Name, err)
		return
	}

	joinSubnets, err := config.GetJoinSubnets(nodeId)
	if err != nil {
		klog.Infof("Failed to parse node %s join subnets annotation %v", node.Name, err)
		return
	}

	ni := nodeInfo{
		name:           node.Name,
		isNodeGlobalAz: util.IsNodeGlobalAz(node),
		id:             nodeId,
		tsMac:          tsMac,
		tsNet:          tsIp.String() + "/16",
		tsIp:           tsIp.String(),
		chassisID:      chassisID,
		nodeSubnets:    nodeSubnets,
		nodePrimaryIp:  nodePrimaryIp,
		joinSubnets:    joinSubnets,
		nodeGRIPs:      nodeGRIPs,
	}

	ic.Lock()
	defer ic.Unlock()
	if existing, ok := ic.nodes[node.Name]; ok {
		if reflect.DeepEqual(existing, ni) {
			return
		}
	}

	ic.nodes[node.Name] = ni
}

func (ic *Controller) createLocalAzNodeResources(ni nodeInfo) error {
	logicalSwitch := nbdb.LogicalSwitch{
		Name: types.TransitSwitch,
		OtherConfig: map[string]string{
			"interconn-ts":             types.TransitSwitch,
			"requested-tnl-key":        transitSwitchTunnelKey,
			"mcast_snoop":              "true",
			"mcast_flood_unregistered": "true",
		},
	}
	if err := libovsdbops.CreateOrUpdateLogicalSwitch(ic.nbClient, &logicalSwitch); err != nil {
		return fmt.Errorf("failed to create/update logical transit switch: %v", err)
	}

	logicalRouterPortName := types.RouterToTransitSwitchPrefix + ni.name
	logicalRouterPort := nbdb.LogicalRouterPort{
		Name:     logicalRouterPortName,
		MAC:      ni.tsMac.String(),
		Networks: []string{ni.tsNet},
		Options: map[string]string{
			"mcast_flood": "true",
		},
	}
	logicalRouter := nbdb.LogicalRouter{
		Name: types.OVNClusterRouter,
	}
	if err := libovsdbops.CreateOrUpdateLogicalRouterPorts(ic.nbClient, &logicalRouter,
		[]*nbdb.LogicalRouterPort{&logicalRouterPort}); err != nil {
		return fmt.Errorf("failed to create/update cluster router to transit switch ports: %v", err)
	}

	lspOptions := map[string]string{
		"router-port":       types.RouterToTransitSwitchPrefix + ni.name,
		"requested-tnl-key": strconv.Itoa(ni.id),
	}

	return ic.addNodeLogicalSwitchPort(types.TransitSwitch, types.TransitSwitchToRouterPrefix+ni.name,
		"router", []string{"router"}, lspOptions)
}

func (ic *Controller) addNodeLogicalSwitchPort(logicalSwitchName, portName, portType string, addresses []string, options map[string]string) error {
	logicalSwitch := nbdb.LogicalSwitch{
		Name: types.TransitSwitch,
	}

	logicalSwitchPort := nbdb.LogicalSwitchPort{
		Name:      portName,
		Type:      portType,
		Options:   options,
		Addresses: addresses,
	}
	if err := libovsdbops.CreateOrUpdateLogicalSwitchPortsOnSwitch(ic.nbClient, &logicalSwitch, &logicalSwitchPort); err != nil {
		return fmt.Errorf("failed to add logical port %s to switch %s, error: %v", portName, logicalSwitch.Name, err)
	}
	return nil
}

func (ic *Controller) createRemoteAzNodeResources(ni nodeInfo) error {
	remotePortAddr := ni.tsMac.String() + " " + ni.tsNet
	lspOptions := map[string]string{
		"requested-tnl-key": strconv.Itoa(ni.id),
	}
	if err := ic.addNodeLogicalSwitchPort(types.TransitSwitch, types.TransitSwitchToRouterPrefix+ni.name, "remote", []string{remotePortAddr}, lspOptions); err != nil {
		return err
	}
	// Set the port binding chassis.
	if err := ic.SetRemotePortBindingChassis(types.TransitSwitchToRouterPrefix+ni.name, ni); err != nil {
		return err
	}

	if err := ic.addRemoteNodeStaticRoutes(ni); err != nil {
		return err
	}

	logicalRouterPort := nbdb.LogicalRouterPort{
		Name: types.RouterToTransitSwitchPrefix + ni.name,
	}
	ctx, cancel := context.WithTimeout(context.Background(), ovntypes.OVSDBTimeout)
	defer cancel()
	if err := ic.nbClient.Get(ctx, logicalRouterPort); err != nil {
		return nil
	}

	logicalRouter := nbdb.LogicalRouter{
		Name: types.OVNClusterRouter,
	}
	if err := libovsdbops.DeleteLogicalRouterPorts(ic.nbClient, &logicalRouter, &logicalRouterPort); err != nil {
		return fmt.Errorf("failed to delete logical router port %s from router %s, error: %v", logicalRouterPort.Name, logicalRouter.Name, err)
	}

	return nil
}

func (ic *Controller) addRemoteNodeStaticRoutes(ni nodeInfo) error {
	addRoute := func(subnet *net.IPNet) error {
		logicalRouterStaticRoute := nbdb.LogicalRouterStaticRoute{
			ExternalIDs: map[string]string{
				"ic-node": ni.name,
			},
			Nexthop:  ni.tsIp,
			IPPrefix: subnet.String(),
		}
		p := func(lrsr *nbdb.LogicalRouterStaticRoute) bool {
			return lrsr.IPPrefix == subnet.String() &&
				lrsr.Nexthop == ni.tsIp &&
				lrsr.ExternalIDs["ic-node"] == ni.name
		}
		//FIXME: All routes should be added in a single transaction instead.
		if err := libovsdbops.CreateOrUpdateLogicalRouterStaticRoutesWithPredicate(ic.nbClient, types.OVNClusterRouter, &logicalRouterStaticRoute, p); err != nil {
			return fmt.Errorf("failed to create static route: %v", err)
		}
		return nil
	}

	//FIXME: No consistency on transaction failure.
	for _, subnet := range ni.nodeSubnets {
		if err := addRoute(subnet); err != nil {
			return fmt.Errorf("unable to create static routes: %v", err)
		}
	}

	for _, subnet := range ni.joinSubnets {
		if err := addRoute(subnet); err != nil {
			return fmt.Errorf("unable to create static routes: %v", err)
		}
	}

	for _, subnet := range ni.nodeGRIPs {
		if err := addRoute(subnet); err != nil {
			return fmt.Errorf("unable to create static routes: %v", err)
		}
	}

	return nil
}

func (ic *Controller) SetRemotePortBindingChassis(portName string, ni nodeInfo) error {
	remotePort := sbdb.PortBinding{
		LogicalPort: portName,
	}
	chassis := sbdb.Chassis{
		Hostname: ni.name,
		Name:     ni.chassisID,
	}

	if err := libovsdbops.UpdatePortBindingSetChassis(ic.sbClient, &remotePort, &chassis); err != nil {
		return fmt.Errorf("failed to update chassis for remote port, error: %v", portName)
	}

	return nil
}

func (ic *Controller) isNodeRemoteAz(ni nodeInfo) bool {
	return !(ni.name == ic.localAzName || (ic.localAzName == types.GlobalAz && ni.isNodeGlobalAz))
}

func (ic *Controller) syncNodeResources(ni nodeInfo) {
	if ic.isNodeRemoteAz(ni) {
		err := ic.createRemoteAzNodeResources(ni)
		if err != nil {
			klog.Errorf("Failed to create remote az node resources, error: %v", err)
		}
	} else {
		err := ic.createLocalAzNodeResources(ni)
		if err != nil {
			klog.Errorf("Failed to create local az node resources, error: %v", err)
		}
	}
}

//func (ic *Controller) cleanupNodeResources(ni nodeInfo) {
//	if ni.name != ic.localAzName {
//		//TODO
//	}
//}

func (ic *Controller) syncNodeChassis(ni nodeInfo) error {
	if ic.isNodeRemoteAz(ni) {
		return ic.syncRemoteAzChassis(ni)
	} else {
		return ic.syncLocalAzChassis(ni)
	}
}

func (ic *Controller) syncRemoteAzChassis(ni nodeInfo) error {
	if err := libovsdbops.CreateOrUpdateRemoteChassis(ic.sbClient, ni.name, ni.chassisID, ni.nodePrimaryIp); err != nil {
		return fmt.Errorf("failed to create encaps and remote chassis %s, error: %v", ni.name, err)
	}
	return nil
}

func (ic *Controller) syncLocalAzChassis(ni nodeInfo) error {
	if err := libovsdbops.UpdateChassisToLocal(ic.sbClient, ni.name, ni.chassisID); err != nil {
		return fmt.Errorf("failed to create update remote chassis into local %s, error: %v", ni.name, err)
	}
	return nil
}
