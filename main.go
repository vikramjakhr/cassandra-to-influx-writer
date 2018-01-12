package main

import (
	"fmt"
	"log"

	"github.com/gocql/gocql"
	"time"
	"github.com/influxdata/influxdb/client/v2"
	"encoding/json"
	"os"
	"os/signal"
)

type Session struct {
	session_id  string
	client      string
	command     string
	coordinator string
	duration    int
	parameters  string
	request     string
	started_at  time.Time
}

func main() {
	log.Println("Started cassandra to influx writer")

	// handle os signals
	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt)
	go func() {
		<-signals
		log.Println("OS Interrupt. Shutting down cassandra to influx writer.")
		os.Exit(0)
	}()

	// connect to the cassandra cluster
	cluster := gocql.NewCluster("1.0.4.95")
	cluster.Timeout = 6000 * time.Millisecond
	cluster.ConnectTimeout = 6000 * time.Millisecond
	cluster.Keyspace = "system_traces"
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		fmt.Println("Error")
		log.Fatal(err)
	}
	defer session.Close()

	// create influx client
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		log.Println("Error while connecting to influx")
		log.Fatal(err)
	}

	ticker := time.NewTicker(time.Minute * 5)

	//go func() {
	for t := range ticker.C {
		log.Printf("Writing cassandra point after interval %s", t)
		// Create a new point batch
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database: "cassandra_system_traces",
		})

		if err != nil {
			log.Println("Error while batch point")
			log.Fatal(err)
		}

		var session_id, clnt, command, coordinator, request string
		var started_at time.Time
		var parameters map[string]string
		var duration int

		tags := make(map[string]string)
		fields := make(map[string]interface{})
		var count int

		// list all sessions
		iter := session.Query(`SELECT session_id,client,command,coordinator,parameters,duration,request,started_at FROM sessions`).Iter()
		for iter.Scan(&session_id, &clnt, &command, &coordinator, &parameters, &duration, &request, &started_at) {
			tags["session_id"] = session_id
			tags["client"] = clnt
			tags["command"] = command
			tags["coordinator"] = coordinator
			fields["duration"] = duration
			param, _ := json.Marshal(parameters)
			fields["parameters"] = string(param)
			fields["request"] = request

			// Create a new point
			pt, err := client.NewPoint("sessions", tags, fields, started_at)
			if err != nil {
				log.Println("Error while new point")
				log.Fatal(err)
			}
			if count == 1000 {
				// Write the batch
				if err := c.Write(bp); err != nil {
					log.Println("Error while write")
					log.Fatal(err)
				}
				count = 0
			} else {
				bp.AddPoint(pt)
				count++
			}
		}

		if count > 0 {
			// Write the batch
			if err := c.Write(bp); err != nil {
				log.Println("Error while write")
				log.Fatal(err)
			}
		}
		if err := iter.Close(); err != nil {
			log.Println("Error while closing iter")
			log.Fatal(err)
		}
	}

	log.Println("Shutting down cassandra to influx writer")
}
