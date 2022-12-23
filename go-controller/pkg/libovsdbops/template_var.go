package libovsdbops

import (
	"context"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	libovsdb "github.com/ovn-org/libovsdb/ovsdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
)

// ListTemplates looks up all chassis template variables.
func ListTemplates(nbClient libovsdbclient.Client) ([]*nbdb.ChassisTemplateVar, error) {
	ctx, cancel := context.WithTimeout(context.Background(), types.OVSDBTimeout)
	defer cancel()

	templatesList := []*nbdb.ChassisTemplateVar{}
	err := nbClient.List(ctx, &templatesList)
	if err != nil {
		return nil, err
	}
	return templatesList, nil
}

func mutateChassisTemplateVarOps(nbClient libovsdbclient.Client, preserveVarValues bool,
	ops []libovsdb.Operation, template *nbdb.ChassisTemplateVar) ([]libovsdb.Operation, error) {

	modelClient := newModelClient(nbClient)
	return modelClient.CreateOrUpdateOps(ops, []operationModel{
		{
			Model: template,
			ModelPredicate: func(item *nbdb.ChassisTemplateVar) bool {
				return item.Chassis == template.Chassis
			},
			OnModelMutations:                  []interface{}{&template.Variables},
			ErrNotFound:                       false,
			BulkOp:                            false,
			PreserveExistingMapValuesOnMutate: preserveVarValues,
		},
	}...)
}

func CreateChassisTemplateVarOps(nbClient libovsdbclient.Client,
	ops []libovsdb.Operation, template *nbdb.ChassisTemplateVar) ([]libovsdb.Operation, error) {

	return mutateChassisTemplateVarOps(nbClient, true, ops, template)
}

func CreateOrUpdateChassisTemplateVarOps(nbClient libovsdbclient.Client,
	ops []libovsdb.Operation, template *nbdb.ChassisTemplateVar) ([]libovsdb.Operation, error) {

	return mutateChassisTemplateVarOps(nbClient, false, ops, template)
}

func DeleteChassisTemplateVarOps(nbClient libovsdbclient.Client,
	ops []libovsdb.Operation, template *nbdb.ChassisTemplateVar) ([]libovsdb.Operation, error) {
	deleteTemplate := &nbdb.ChassisTemplateVar{
		Chassis:   template.Chassis,
		Variables: map[string]string{},
	}
	for name := range template.Variables {
		deleteTemplate.Variables[name] = ""
	}
	modelClient := newModelClient(nbClient)
	return modelClient.DeleteOps(ops, []operationModel{
		{
			Model: deleteTemplate,
			ModelPredicate: func(item *nbdb.ChassisTemplateVar) bool {
				return item.Chassis == deleteTemplate.Chassis
			},
			OnModelMutations: []interface{}{&deleteTemplate.Variables},
			ErrNotFound:      false,
			BulkOp:           false,
		},
	}...)
}

type chassisTemplateVarPredicate func(*nbdb.ChassisTemplateVar) bool

func DeleteChassisTemplateVarValues(nbClient libovsdbclient.Client, model *nbdb.ChassisTemplateVar, predicate chassisTemplateVarPredicate) error {
	opModel := operationModel{
		Model:            model,
		ModelPredicate:   predicate,
		OnModelMutations: []interface{}{&model.Variables},
		ErrNotFound:      false,
		BulkOp:           true,
	}

	m := newModelClient(nbClient)
	return m.Delete(opModel)
}

// DeleteChassisTemplateRecord deletes all complete Chassis_Template_Var
// records matching 'templates'.
func DeleteChassisTemplateRecord(nbClient libovsdbclient.Client, templates ...*nbdb.ChassisTemplateVar) error {
	opModels := make([]operationModel, 0, len(templates))
	for _, template := range templates {
		opModels = append(opModels, operationModel{
			Model: template,
			ModelPredicate: func(item *nbdb.ChassisTemplateVar) bool {
				return item.Chassis == template.Chassis
			},
			ErrNotFound: false,
			BulkOp:      true,
		})
	}
	m := newModelClient(nbClient)
	return m.Delete(opModels...)
}
