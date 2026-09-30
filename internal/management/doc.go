// Package management serves the management REST API, which inspects a Francis cluster and runs a small set of audited administrative actions
package management

// The general information of the OpenAPI document, which `make gen-openapi` generates from the swag annotations in this package into openapi/openapi.yaml
// The long description of the API is in openapi/api.md
// The security definition must come last, because swag reads every line after it as one of its attributes
//
//	@title		Francis management API
//	@version	v1
//	@BasePath	/
//	@description.markdown
//
//	@tag.name					Meta
//	@tag.description			Public endpoints.
//	@tag.name					Cluster
//	@tag.description			Cluster-wide summary, runtime replicas, and hosts.
//	@tag.name					Actors
//	@tag.description			Activations, placements, actor types, and actor state.
//	@tag.name					Jobs
//	@tag.description			Durable jobs and alarms.
//	@tag.name					Workflows
//	@tag.description			Workflow definitions, instances, and event history.
//	@tag.name					Actions
//	@tag.description			Audited operations that change cluster state.
//
//	@securityDefinitions.apikey	bearerAuth
//	@in							header
//	@name						Authorization
//	@description				A read-only or management token from the server configuration, sent as `Authorization: Bearer <token>`. Read-only tokens grant every scope except those ending in `:manage`; management tokens grant every scope.
