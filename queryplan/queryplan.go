package queryplan

import (
	//	"database/sql"
	//	"time"
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"gopkg.in/redis.v4"
)

func Unmarshal(reader *csv.Reader, v interface{}) error {
	record, err := reader.Read()
	if err != nil {
		return err
	}
	s := reflect.ValueOf(v).Elem()
	if s.NumField() != len(record) {
		return &FieldMismatch{s.NumField(), len(record)}
	}
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		switch f.Type().String() {
		case "string":
			f.SetString(record[i])
		case "int":
			ival, err := strconv.ParseInt(record[i], 10, 0)
			if err != nil {
				return err
			}
			f.SetInt(ival)
		case "float64":
			fval, err := strconv.ParseFloat(record[i], 64)
			if err != nil {
				return err
			}
			f.SetFloat(fval)
		default:
			return &UnsupportedType{f.Type().String()}
		}
	}
	return nil
}

type PlannedQuery struct {
	ProxyURL string
	Username string
	Password string
	Lng      float64
	Lat      float64
}

type QueryPoint struct {
	Lat      float64
	Lng      float64
	ProxyURL string
}

func StoreWorker(red *redis.Client, pq PlannedQuery) {
	bPlannedQuery, _ := json.Marshal(pq)
	red.HSet("workers", pq.Username, string(bPlannedQuery))
}

func StorePlannedQueryPoint(red *redis.Client, pq PlannedQuery) {
	querypoint := QueryPoint{Lat: pq.Lat, Lng: pq.Lng, ProxyURL: pq.ProxyURL}
	b, _ := json.Marshal(querypoint)
	red.ZAdd("queryplan:"+pq.Username, redis.Z{Score: 0, Member: string(b)})
}

func StoreQueryPointWithTime(red *redis.Client, username string, querypoint QueryPoint) {
	b, _ := json.Marshal(querypoint)
	red.ZAdd("queryplan:"+username, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: string(b)})
}

func GetQueryPoint(waitTillRequestSeconds float64, red *redis.Client, username string) ([]QueryPoint, error) {
	neverScannedRangeQueryBy := redis.ZRangeBy{
		Min:    "0",
		Max:    "0",
		Offset: 0,
		Count:  10}

	neverScanned, err := red.ZRangeByScore(
		"queryplan:"+username,
		neverScannedRangeQueryBy).Result()

	currentMinusSevenPtFiveMin := fmt.Sprintf("%f",
		float64(time.Now().Unix())-(waitTillRequestSeconds*60))

	notRecentlyScannedRangeQueryBy := redis.ZRangeBy{
		Min:    "1",
		Max:    currentMinusSevenPtFiveMin,
		Offset: 0,
		Count:  10}

	notRecentlyScanned, err := red.ZRangeByScore(
		"queryplan:"+username,
		notRecentlyScannedRangeQueryBy).Result()

	results := append(neverScanned, notRecentlyScanned...)

	querypoints := []QueryPoint{}
	var deserialized QueryPoint
	for _, r := range results {
		err = json.Unmarshal([]byte(r), &deserialized)
		if err != nil {
			return nil, err
		}
		querypoints = append(querypoints, deserialized)
	}
	return querypoints, nil
}

func GetWorkers(red *redis.Client) ([]PlannedQuery, error) {
	workers := []PlannedQuery{}

	results, err := red.HGetAll("workers").Result()
	if err != nil {
		return nil, err
	}

	var deserialized PlannedQuery
	for _, r := range results {
		fmt.Println(r)
		err = json.Unmarshal([]byte(r), &deserialized)
		if err != nil {
			return nil, err
		}
		workers = append(workers, deserialized)
	}

	return workers, nil
}

func HydrateQueryPlan(queryPlanFilePath string, red *redis.Client) {
	file, err := os.Open(queryPlanFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))
	var query PlannedQuery
	for {
		err := Unmarshal(reader, &query)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("storing ", query)
		StoreWorker(red, query)
		StorePlannedQueryPoint(red, query)

		//fmt.Printf("%s %s has age of %d\n", test.Name, test.Surname, test.Age)
	}
}

type FieldMismatch struct {
	expected, found int
}

func (e *FieldMismatch) Error() string {
	return "CSV line fields mismatch. Expected " + strconv.Itoa(e.expected) + " found " + strconv.Itoa(e.found)
}

type UnsupportedType struct {
	Type string
}

func (e *UnsupportedType) Error() string {
	return "Unsupported type: " + e.Type
}
