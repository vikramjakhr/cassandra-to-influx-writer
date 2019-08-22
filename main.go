package main

import (
	"log"

	"github.com/gocql/gocql"
	"time"
	"github.com/influxdata/influxdb/client/v2"
	"encoding/json"
	"os"
	"os/signal"
	"strings"
	"flag"
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
	handleOSSignals()
	log.Println("Parsing command line arguments. Example: -nodes=1.0.4.95,1.0.4.96 ")
	nodes := flag.String("nodes", "", "cassandra cluster nodes address")
	flag.Parse()
	log.Println("Validating command line arguments.")
	n := strings.Trim(*nodes, " ")
	if n == "" {
		log.Printf("Nodes value cannot be null or empty. Found %s", n)
		log.Fatal("Exiting now.")
	}

	addresses := strings.Split(n, ",")
	log.Printf("Cluster nodes after split: %s", addresses)

	// connect to the cassandra cluster
	log.Printf("Connecting to cassandra cluster with %s", addresses)
	cluster := gocql.NewCluster(addresses...)
	cluster.Timeout = 60000 * time.Millisecond
	cluster.ConnectTimeout = 60000 * time.Millisecond
	cluster.Keyspace = "system_traces"
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		log.Println("Error while creating cassandra session.")
		log.Fatal(err)
	}
	log.Println("Connected to cassandra cluster.")
	defer session.Close()

	influxDB := "Connecto_cassandra"

	// create influx client
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		log.Println("Error while connecting to influx")
		log.Fatal(err)
	}

	ticker := time.NewTicker(time.Minute * 5)

	for t := range ticker.C {
		log.Printf("Runing ticker after interval %s", t)
		/*var last string
		res, err := queryInfluxDB(c, influxDB, fmt.Sprintf("select last(%s) from sessions;", "started_at"))
		if err != nil {
			log.Printf("Error while fetching last record from influxdb");
			log.Println(err)
			continue
		} else {
			log.Printf("Influx last record: %s", res)
			if len(res) > 0 && len(res[0].Series) > 0 && len(res[0].Series[0].Values) > 0 {
				log.Printf("Influx last record: %s", res[0])
				tym := res[0].Series[0].Values[0][0]
				val := tym.(string)
				val, ok := tym.(string)
				if ok {
					last = val
				} else {
					log.Println("Error while parsing last record time from influx")
					log.Fatal(err)
				}
			}
		}
		log.Printf("Writing cassandra point after %s", last)*/
		// Create a new point batch
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database: influxDB,
		})

		if err != nil {
			log.Println("Error while creating new influx batch point")
			log.Fatal(err)
		}

		var session_id, clnt, command, coordinator, request string
		var started_at time.Time
		var parameters map[string]string
		var duration int

		tags := make(map[string]string)
		fields := make(map[string]interface{})
		var count int

		/*csQuery := fmt.Sprintf("SELECT session_id,client,command,coordinator,parameters,duration,request,started_at "+
			"FROM sessions where started_at > '%s' ALLOW FILTERING", last)*/

		csQuery := "SELECT session_id,client,command,coordinator,parameters,duration,request,started_at FROM sessions"

		log.Printf("Executing cassandra Query: %s", csQuery)

		// list all sessions
		iter := session.
		Query(csQuery).
			PageSize(100000).
			Iter()
		for iter.Scan(&session_id, &clnt, &command, &coordinator, &parameters, &duration, &request, &started_at) {
			tags["session_id"] = session_id
			tags["client"] = clnt
			tags["command"] = command
			tags["coordinator"] = coordinator
			fields["duration"] = duration
			fields["session_id"] = session_id
			param, _ := json.Marshal(parameters)
			s := string(param)
			fields["parameters"] = s
			fields["request"] = request
			fields["started_at"] = started_at

			if strings.Contains(s, "SELECT") {
				tags["type"] = "SELECT"
			} else if strings.Contains(s, "INSERT") {
				tags["type"] = "INSERT"
			} else if strings.Contains(s, "UPDATE") {
				tags["type"] = "UPDATE"
			} else {
				continue
			}

			pt, err := client.NewPoint("system_traces_sessions", tags, fields, started_at)
			if err != nil {
				log.Println("Error while creating new influx batch point.")
			}
			if count == 1000 {
				// Write the batch
				if err := c.Write(bp); err != nil {
					log.Println("Error while writing new influx batch.")
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
				log.Println("Error while writing new influx batch.")
			}
		}
		if err := iter.Close(); err != nil {
			log.Println("Error while closing cassandra iterator")
			session.Close()
			log.Fatal(err)
		} else {
			session.Query("TRUNCATE sessions").Exec()
		}
	}
	log.Println("Shutting down cassandra to influx writer")
}

func queryInfluxDB(clnt client.Client, db, cmd string) (res []client.Result, err error) {
	log.Println("Querying influx")
	q := client.Query{
		Command:  cmd,
		Database: db,
	}
	if response, err := clnt.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}

func handleOSSignals() {
	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt)
	go func() {
		<-signals
		log.Println("OS Interrupt. Shutting down cassandra to influx writer.")
		os.Exit(0)
	}()
}
