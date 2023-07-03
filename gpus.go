/* Copyright 2020 Joeri Hermans, Victor Penso, Matteo Dessalvi

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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"io/ioutil"
	"os/exec"
	"strconv"
	"strings"
)

type GPUsMetrics struct {
	alloc       float64
	idle        float64
	total       float64
	utilization float64
}

func GPUsGetMetrics(cluster ClusterInfo) *GPUsMetrics {
	return ParseGPUsMetrics(cluster)
}

func ParseAllocatedGPUs(cluster ClusterInfo) (map[string]float64, float64) {
	var num_gpus = 0.0
	var gpus_pertype = make(map[string]float64)

	args := append(cluster.cmdargs, "-a", "-X", "--format=AllocTRES", "--state=RUNNING", "--noheader", "--parsable2")
	output := string(Execute("sacct", args))
	if len(output) > 0 {
		for _, line := range strings.Split(output, "\n") {
			line = strings.Trim(line, "\"")
			if strings.HasPrefix(line, "board:") {
				boardinfo := strings.Split(line, ":")
				btype := boardinfo[1]
				job_gpus, _ := strconv.ParseFloat(boardinfo[2], 64)
				num_gpus += job_gpus
				gpus_pertype[btype] += job_gpus
			}
		}
	}

	return gpus_pertype, num_gpus
}

func ParseTotalGPUs(cluster ClusterInfo) (map[string]float64, float64) {
	var num_gpus = 0.0
	var gpus_pertype = make(map[string]float64)

	args := append(cluster.cmdargs, "-h", "-o \"%n %G\"")
	output := string(Execute("sinfo", args))
	if len(output) > 0 {
		for _, line := range strings.Split(output, "\n") {
			line = strings.Trim(line, "\"")
			fields := strings.Fields(line)
			if len(fields) > 1 && strings.HasPrefix(fields[1], "board:") {
				boardinfo := strings.Split(fields[1], ":")
				btype := boardinfo[1]
				node_gpus, _ := strconv.ParseFloat(boardinfo[2], 64)
				num_gpus += node_gpus
				gpus_pertype[btype] += node_gpus
			}
		}
	}

	return gpus_pertype, num_gpus
}

func ParseGPUsMetrics(cluster ClusterInfo) *GPUsMetrics {
	var gm GPUsMetrics
	_, total_gpus := ParseTotalGPUs(cluster)
	_, total_alloc := ParseAllocatedGPUs(cluster)
	gm.alloc = total_alloc
	gm.idle = total_gpus - total_alloc
	gm.total = total_gpus
	gm.utilization = total_alloc / total_gpus
	return &gm
}

// Execute the sinfo command and return its output
func Execute(command string, arguments []string) []byte {
	cmd := exec.Command(command, arguments...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
	return out
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewGPUsCollector() *GPUsCollector {
	labels := []string{"cluster"}
	return &GPUsCollector{
		alloc:       prometheus.NewDesc("slurm_gpus_alloc", "Allocated GPUs", labels, nil),
		idle:        prometheus.NewDesc("slurm_gpus_idle", "Idle GPUs", labels, nil),
		total:       prometheus.NewDesc("slurm_gpus_total", "Total GPUs", labels, nil),
		utilization: prometheus.NewDesc("slurm_gpus_utilization", "Total GPU utilization", labels, nil),
	}
}

type GPUsCollector struct {
	alloc       *prometheus.Desc
	idle        *prometheus.Desc
	total       *prometheus.Desc
	utilization *prometheus.Desc
}

// Send all metric descriptions
func (cc *GPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.alloc
	ch <- cc.idle
	ch <- cc.total
	ch <- cc.utilization
}
func (cc *GPUsCollector) Collect(ch chan<- prometheus.Metric) {
	for _, c := range clusters {
		cm := GPUsGetMetrics(c)
		ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, cm.alloc, c.name)
		ch <- prometheus.MustNewConstMetric(cc.idle, prometheus.GaugeValue, cm.idle, c.name)
		ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, cm.total, c.name)
		ch <- prometheus.MustNewConstMetric(cc.utilization, prometheus.GaugeValue, cm.utilization, c.name)
	}
}
