// Package clusterhealth holds an exploratory end-to-end test of the YBA universe cluster-health
// API, gauge and safe-to-take-down latency histogram. The test is behind the itest build tag;
// this untagged file keeps `go vet ./...` working without the tag. See README.md.
package clusterhealth
