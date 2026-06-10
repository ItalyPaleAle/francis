//go:build integration

// Package clustersecret holds the fixed cluster secrets shared across integration test processes
// The runtime derives its CA from RuntimePSK and accepts remote hosts that present HostBootstrapPSK, while local hosts self-issue from RuntimePSK
package clustersecret

// RuntimePSK derives the cluster CA: the runtime and every local host share it
var RuntimePSK = []byte("francis-integration-runtime-psk-0123456789")

// HostBootstrapPSK authenticates joining remote hosts: the runtime and every remote host share it
var HostBootstrapPSK = []byte("francis-integration-host-bootstrap-psk-0123456789")
