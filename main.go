/* Copyright 2017-2020 Victor Penso, Matteo Dessalvi, Joeri Hermans

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
	"flag"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"net/http"
	"os/exec"
	"strings"
)

type ClusterInfo struct {
	name    string
	cmdargs []string
}

var allclusters = map[string][]string{"local": nil}

func init() {
	// Metrics have to be registered to be exposed
	prometheus.MustRegister(NewAccountsCollector())   // from accounts.go
	prometheus.MustRegister(NewCPUsCollector())       // from cpus.go
	prometheus.MustRegister(NewNodesCollector())      // from nodes.go
	prometheus.MustRegister(NewNodeCollector())       // from node.go
	prometheus.MustRegister(NewPartitionsCollector()) // from partitions.go
	prometheus.MustRegister(NewQueueCollector())      // from queue.go
	prometheus.MustRegister(NewSchedulerCollector())  // from scheduler.go
	prometheus.MustRegister(NewFairShareCollector())  // from sshare.go
	prometheus.MustRegister(NewUsersCollector())      // from users.go

	// Get the available clusters from 'sacctmgr list cluster'
	// But there could be clusters in there that aren't valid.
	// Get through and vet them by using a simple "sshare -M <cluster>"
	// and drop them if it failed.
	valid := []string{}
	args := []string{"list", "cluster", "format=Cluster", "-n", "-P"}
	output := strings.TrimSpace(string(Execute("sacctmgr", args)))
	for _, c := range strings.Split(output, "\n") {
		cmd := exec.Command("sshare", "-M", c)
		cmd.Start() // We'll catch any error at Wait()
		if err := cmd.Wait(); err == nil {
			allclusters[c] = []string{"-M", c}
			valid = append(valid, c)
		}
	}
	available := strings.Join(valid, ",")
	flag.String(
		"clusters",
		"local",
		fmt.Sprintf("List of clusters to export. Available: %v", available))
}

var listenAddress = flag.String(
	"listen-address",
	":9341",
	"The address to listen on for HTTP requests.")

var gpuAcct = flag.Bool(
	"gpus-acct",
	false,
	"Enable GPUs accounting")

var clusters []ClusterInfo

func main() {
	flag.Parse()

	carg := flag.Lookup("clusters").Value.String()
	for _, c := range strings.Split(carg, ",") {
		if _, ok := allclusters[c]; !ok {
			log.Fatalf("Unknown cluster '%s' specified. See --help for list.", c)
		}
		clusters = append(clusters, ClusterInfo{c, allclusters[c]})
	}

	// Turn on GPUs accounting only if the corresponding command line option is set to true.
	if *gpuAcct {
		prometheus.MustRegister(NewGPUsCollector()) // from gpus.go
	}

	// The Handler function provides a default handler to expose metrics
	// via an HTTP server. "/metrics" is the usual endpoint for that.
	log.Infof("Starting Server: %s", *listenAddress)
	log.Infof("GPUs Accounting: %t", *gpuAcct)
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
