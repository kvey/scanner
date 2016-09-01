package main

import (
	//"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"time"
	//"strconv"

	"golang.org/x/net/context"

	_ "github.com/lib/pq"
	"github.com/pogodevorg/pgoapi-go/api"
	"github.com/pogodevorg/pgoapi-go/auth"
	"github.com/zeeraw/encrypt"
	"gopkg.in/redis.v4"

	"github.com/kvey/mapper/feeds"
	"github.com/kvey/mapper/queryplan"
)

const (
	DB_USER     = "pokemon"
	DB_PASSWORD = "pokemondb4321"
	//vq9Yn1Fy0zzn
	DB_NAME = "pokemon"
	DB_HOST = "45.79.99.82"
)

type ScannedMon struct {
	ScannedMonId            int
	PokemonId               int
	SpawnPointId            string
	EncounterId             int64
	Lat                     float64
	Lng                     float64
	LastModifiedTimestampMs int64
	TimeTillHiddenMs        int64
}

// Call this worker to validate an account:proxy list
// - will re-authenticate on a new proxy for each point, assuming appropriate input format
func workerProxyTest(feed api.Feed, crypto api.Crypto, tasks <-chan int, username string, password string, f *os.File) {
	red := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	ctx := context.Background()

	// Initialize a new authentication provider to retrieve an access token
	provider, err := auth.NewProvider("ptc", username, password)
	if err != nil {
		fmt.Println(err)
		return
	}

	session := api.NewSession(provider, &api.Location{}, feed, crypto, false)

	for {
		task, ok := <-tasks
		if !ok {
			return
		}

		points, err := queryplan.GetQueryPoint(float64(7.5), red, username)
		if err != nil {
			fmt.Println(err)
			return
		}

		p := points[0]

		location := &api.Location{
			Lon: p.Lng,
			Lat: p.Lat,
			Alt: 1.0,
		}

		// change to new proxyURL
		rpc, err := api.ProxyRPC(p.ProxyURL)
		session.SetRPC(rpc)
		if err != nil {
			fmt.Println(err)
			return
		}

		session.MoveTo(location)

		fmt.Printf("Logging in: %s at %s\n", username, p.ProxyURL)
		err = session.Init(ctx)
		if err != nil {
			fmt.Println(err)
			continue
		}

		objects, err := session.GetPlayerMap(ctx)
		if err != nil {
			fmt.Println(err)
			continue
		}

		outObjects, err := json.Marshal(objects)
		if err != nil {
			fmt.Println(err)
			continue
		}

		cells := objects.GetMapCells()
		for _, cell := range cells {
			if len(cell.GetForts()) > 0 {
				if _, err = f.WriteString(p.ProxyURL + "\n"); err != nil {
					panic(err)
				}
			}
		}

		fmt.Println(string(outObjects))
		fmt.Println(task)
	}
}

// call this worker to query an account,proxy,lat,lng list
func worker(feed api.Feed, crypto api.Crypto, tasks <-chan int, username string, password string, proxyURL string, f *os.File) {
	red := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	ctx := context.Background()

	// Initialize a new authentication provider to retrieve an access token
	provider, err := auth.NewProvider("ptc", username, password)
	if err != nil {
		fmt.Println(err)
		return
	}

	// this worker will use the first proxy that appeared per account for that account
	session := api.NewSession(provider, &api.Location{}, feed, crypto, false)

	//rpc, err := api.ProxyRPC(proxyURL)
	//session.SetRPC(rpc)
	if err != nil {
		fmt.Println(err)
		return
	}

	isSessionInit := false

	for {
		task, ok := <-tasks
		if !ok {
			return
		}

		points, err := queryplan.GetQueryPoint(float64(7.5), red, username)
		if err != nil {
			fmt.Println(err)
			return
		}

		p := points[0]

		location := &api.Location{
			Lon: p.Lng,
			Lat: p.Lat,
			Alt: 1.0,
		}

		if !isSessionInit {
			session.MoveTo(location)
			fmt.Printf("Logging in: %s at %s\n", username, proxyURL)
			err = session.Init(ctx)
			if err != nil {
				fmt.Println(err)
				continue
			} else {
				isSessionInit = true
			}
		} else {
			session.MoveTo(location)
		}

		objects, err := session.GetPlayerMap(ctx)
		if err != nil {
			fmt.Println(err)
			continue
		}

		outObjects, err := json.Marshal(objects)
		if err != nil {
			fmt.Println(err)
			continue
		}

		cells := objects.GetMapCells()
		for _, cell := range cells {
			if len(cell.GetForts()) > 0 {
				// record proxy urls successfully queried to file
				if _, err = f.WriteString(proxyURL + "\n"); err != nil {
					panic(err)
				}
			}
		}

		fmt.Println(string(outObjects))

		queryplan.StoreQueryPointWithTime(red, username, p)

		//time.Sleep(time.Duration(task) * time.Millisecond)
		//fmt.Println(points)
		fmt.Println(task)
		fmt.Println(username)
	}
}

func main() {

	red := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	feed := &feeds.DebugFeed{}
	crypto := encrypt.NewCrypto()

	// Populate redis with list of workers - points
	queryplan.HydrateQueryPlan("./scanallocation.csv", red)
	workers, err := queryplan.GetWorkers(red)

	if err != nil {
		fmt.Println(err)
		return
	}

	f, err := os.OpenFile("./mapper-out.txt", os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	tasks := make(chan int)

	for _, pq := range workers {
		// each is a different worker

		///go worker(feed, crypto, tasks, pq.Username, pq.Password, pq.ProxyURL, f)
		go workerProxyTest(feed, crypto, tasks, pq.Username, pq.Password, f)
	}

	// Initial scanning phase - one hour, 10 secs, 7.5 min wait
	ticker := time.NewTicker(time.Second * 10)

	go func() {
		for t := range ticker.C {
			fmt.Println("Tick at", t)
			fmt.Println("Worker count:", len(workers))
			for i := 0; i < len(workers); i++ {
				// each worker is run once
				tasks <- i
			}
		}
	}()

	time.Sleep(time.Second * 240)
	ticker.Stop()
}
