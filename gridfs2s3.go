package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

var (
	access_key string
	secret_key string
	region     string
	mongo_url  string
	mongo_db   string
	mongo_coll string
	workers    int
	date       string
)

const (
	DATE_FORM = "2006-01-02"
)

func init() {
	flag.StringVar(&access_key, "k", "", "access key")
	flag.StringVar(&secret_key, "s", "", "secret key")
	flag.StringVar(&region, "r", "us-east-1", "region")
	flag.StringVar(&date, "t", "", "start date")
	flag.StringVar(&mongo_url, "h", "mongodb://localhost", "MongoDB endpoint (e.g. mongodb://host1:port1,host2:port2)")
	flag.StringVar(&mongo_db, "d", "file", "Gridfs database name")
	flag.StringVar(&mongo_coll, "c", "fs", "Gridfs collection name")
	flag.IntVar(&workers, "w", 1, "parallel workers")
}

func main() {
	flag.Parse()
	check_args()

	fmt.Println("Preparing S3 client")
	auth, err := aws.GetAuth(access_key, secret_key)
	check(err)
	client := s3.New(auth, aws.Regions[region])
	image_bucket := client.Bucket("mico-image")
	video_bucket := client.Bucket("mico-video")
	audio_bucket := client.Bucket("mico-audio")
	fmt.Println("Preparing MongoDB client")
	session, err := mgo.Dial(mongo_url)
	check(err)
	defer session.Close()

	begin, err := time.Parse(DATE_FORM, date)
	check(err)
	dateRange := make([]time.Time, workers+1)
	dateRange[0] = begin
	split := 24 / workers
	for i := 0; i < workers; i++ {
		dateRange[i+1] = begin.Add(time.Duration(split*(i+1)) * time.Hour)
	}

	// // Migrate files
	// gfs := session.DB(mongo_db).GridFS(mongo_coll)
	// iter := gfs.Find(bson.M{"uploadDate": bson.M{"$gte": begin, "$lte": end}}).Iter()
	// total, err := gfs.Find(bson.M{"uploadDate": bson.M{"$gte": begin, "$lte": end}}).Count()
	// check(err)
	// fmt.Println("Migrating", total, "files")

	migrated := make(chan int, workers)
	wg := new(sync.WaitGroup)
	// wg.Add(workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sess := session.Copy()
			defer sess.Close()
			gfs := sess.DB(mongo_db).GridFS(mongo_coll)
			total, err := gfs.Find(bson.M{"uploadDate": bson.M{"$gte": dateRange[id], "$lte": dateRange[id+1]}}).Count()
			check(err)
			fmt.Println("migrating", total, "files, from", dateRange[id], "to", dateRange[id+1])
			iter := gfs.Find(bson.M{"uploadDate": bson.M{"$gte": dateRange[id], "$lte": dateRange[id+1]}}).Iter()
			defer iter.Close()
			var f *mgo.GridFile
			file_count := 0
			for gfs.OpenNext(iter, &f) {
				log.Println("file:", f.Name(), f.ContentType(), f.Size(), f.UploadDate())
				name := f.Name()
				if strings.HasSuffix(name, "_t") || strings.HasSuffix(name, "_thumbnail") {
					log.Println("ignore", name)
					continue
				}
				contentType := f.ContentType()
				if contentType == "" {
					log.Println("missing type", name)
					contentType = "image/png"
				}
				if strings.HasPrefix(contentType, "audio") {
					err := audio_bucket.PutReader(name, f, f.Size(), contentType, s3.PublicRead)
					if err != nil {
						log.Println(id, "upload error", err)
						continue
					}
					log.Println("upload", name, contentType)
					file_count++
				} else if strings.HasPrefix(contentType, "video") {
					err := video_bucket.PutReader(name, f, f.Size(), contentType, s3.PublicRead)
					if err != nil {
						log.Println(id, "upload error", err)
						continue
					}
					log.Println("upload", name, contentType)
					file_count++
				} else if strings.HasPrefix(contentType, "image") {
					err := image_bucket.PutReader(name, f, f.Size(), contentType, s3.PublicRead)
					if err != nil {
						log.Println(id, "upload error", err)
						continue
					}
					log.Println("upload", name, contentType)
					file_count++
				} else {
					log.Println("invalid type", name, contentType)
					continue
				}
			}
			log.Println("error on iter", iter.Err())
			log.Println("worker", id, "migrated", file_count, "files, from", dateRange[id], "to", dateRange[id+1])
			migrated <- file_count
		}(i)
	}

	done := make(chan bool)
	go func() {
		count := 0
		for m := range migrated {
			count += m
		}
		fmt.Println("totally migrated", count, "files")
		done <- true
	}()

	wg.Wait()
	close(migrated)
	<-done
	// check(iter.Close())
}

func check_args() {
	if access_key == "" || secret_key == "" {
		log.Fatal("AWS credentials are required")
	}
	if _, ok := aws.Regions[region]; !ok {
		log.Fatal("Invalid region name")
	}
	if mongo_url == "" {
		log.Fatal("MongoDB connection string is required")
	}
	if mongo_db == "" {
		log.Fatal("MongoDB database name is required")
	}
	if mongo_coll == "" {
		log.Fatal("MongoDB collection name is required")
	}
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
