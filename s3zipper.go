package main

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"net/http"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	redigo "github.com/garyburd/redigo/redis"
)

type configuration struct {
	AccessKey          string
	SecretKey          string
	Bucket             string
	Region             string
	RedisServerAndPort string
	RedisPassword      string
	Port               string
}

var config configuration
var awsBucket *s3.Bucket
var redisPool *redigo.Pool

type redisDLObject struct {
	Name  string
	Files []redisFile
}

type redisFile struct {
	FileName string
	Folder   string
	S3Path   string
	// Optional
	TrackID      int64 `json:",string"`
	PlaylistID   int64 `json:",string"`
	PlaylistName string
}

func main() {
	// if 1 == 0 {
	// 	test()
	// 	return
	// }

	initConfig()
	initAwsBucket()
	initRedis()

	fmt.Println("Running on port:", config.Port)
	http.HandleFunc("/", handler)
	http.ListenAndServe(":"+config.Port, nil)
}

// func test() {
// 	var err error
// 	var files []*redisFile
// 	jsonData := "[{\"S3Path\":\"audio/7W/7W Abandoned.mp3\",\"FileName\":\"7W Abandoned.mp3\",\"Folder\":\"7W\",\"TrackID\":\"4169\",\"PlaylistID\":\"120990\",\"PlaylistName\":\"Test Playlist\"},{\"S3Path\":\"audio/7W/7W Ancient.mp3\",\"FileName\":\"7W Ancient.mp3\",\"Folder\":\"7W/ALT\",\"TrackID\":\"4170\",\"PlaylistID\":\"120990\",\"PlaylistName\":\"Test Playlist\",\"modified\":\"2015-07-18T02:05:04Z\"}]"

// 	resultByte := []byte(jsonData)

// 	err = json.Unmarshal(resultByte, &files)
// 	if err != nil {
// 		err = errors.New("Error decoding json: " + jsonData)
// 	}
// }

func initConfig() {
	defaults := func(value, def string) string {
		if value == "" {
			return def
		}
		return value
	}

	config = configuration{
		AccessKey:          os.Getenv("AWS_ACCESS_KEY"),
		SecretKey:          os.Getenv("AWS_SECRET_KEY"),
		Bucket:             os.Getenv("AWS_BUCKET"),
		Region:             defaults(os.Getenv("AWS_REGION"), "us-east-1"),
		RedisServerAndPort: os.Getenv("REDIS_URL"),
		RedisPassword:      os.Getenv("REDIS_PASSWORD"),
		Port:               defaults(os.Getenv("S3ZIPPER_PORT"), "8000"),
	}
}

func initAwsBucket() {
	fmt.Println("Initializing S3 bucket:", config.Bucket)
	expiration := time.Now().Add(time.Hour * 1)
	auth, err := aws.GetAuth(config.AccessKey, config.SecretKey, "", expiration) //"" = token which isn't needed
	if err != nil {
		panic(err)
	}

	awsBucket = s3.New(auth, aws.GetRegion(config.Region)).Bucket(config.Bucket)
}

func initRedis() {
	fmt.Println("Initializing redis connection.")
	redisPool = &redigo.Pool{
		MaxIdle:     10,
		IdleTimeout: 1 * time.Second,
		Dial: func() (redigo.Conn, error) {
			c, err := redigo.Dial("tcp", config.RedisServerAndPort)
			if err != nil {
				fmt.Println("Error connecting to redis:", err)
				return nil, err
			}
			if auth := config.RedisPassword; auth != "" {
				if _, err := c.Do("AUTH", auth); err != nil {
					c.Close()
					fmt.Println("Redis authentication failed:", err)
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redigo.Conn, t time.Time) (err error) {
			_, err = c.Do("PING")
			if err != nil {
				panic("Error connecting to redis!")
			}
			return
		},
	}
}

// Remove all other unrecognised characters apart from
var makeSafeFileName = regexp.MustCompile(`[#<>:"/\|?*\\]`)

func getFilesFromRedis(ref string) (filesObj redisDLObject, err error) {

	// Testing - enable to test. Remove later.
	// if 1 == 0 && ref == "test" {
	// 	files = append(files, &redisFile{FileName: "test.zip", Folder: "", S3Path: "test/test.zip"}) // Edit and dplicate line to test
	// 	return
	// }

	redis := redisPool.Get()
	defer redis.Close()

	// Get the value from Redis
	result, err := redis.Do("GET", "zip:"+ref)
	if err != nil || result == nil {
		err = errors.New("Access Denied (link expired)")
		return
	}

	// Convert to bytes
	var resultByte []byte
	var ok bool
	if resultByte, ok = result.([]byte); !ok {
		err = errors.New("Error converting data stream to bytes.")
		return
	}

	// Decode JSON
	err = json.Unmarshal(resultByte, &filesObj)
	if err != nil {
		err = errors.New("Error decoding json: " + string(resultByte))
	}

	return
}

func handler(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s\t%s\tSTART", r.Method, r.RequestURI)
	start := time.Now()

	// Get "ref" URL params
	refs, ok := r.URL.Query()["ref"]
	if !ok || len(refs) < 1 {
		http.Error(w, "Missing required parameters. Pass ?ref= to use.", 500)
		return
	}
	ref := refs[0]

	fileObj, err := getFilesFromRedis(ref)
	files := fileObj.Files
	if err != nil {
		http.Error(w, err.Error(), 403)
		log.Printf("%s\t%s\t%s", r.Method, r.RequestURI, err.Error())
		return
	}
	downloadName := fileObj.Name + "-videos"

	// Calculate the total size of the files
	totalZipSize := int64(0)
	for _, file := range files {
		fullpath := awsBucket.URL(file.S3Path)
		resp, err := http.Head(fullpath)
		if err != nil {
			log.Printf("Failed HEAD for file %s", fullpath)
			http.Error(w, "Failed to process files for download", 400)
			return
		}
		totalZipSize += resp.ContentLength
	}

	// Start processing the response
	w.Header().Add("Content-Disposition", "attachment; filename=\""+downloadName+".zip\"")
	w.Header().Add("Content-Type", "application/zip")
	w.Header().Add("Content-Length", strconv.Itoa(int(totalZipSize)))

	// Loop over files, add them to the zip stream
	zipWriter := zip.NewWriter(w)
	for _, file := range files {

		// Build safe file file name
		safeFileName := makeSafeFileName.ReplaceAllString(file.FileName, "")
		if safeFileName == "" { // Unlikely but just in case
			safeFileName = "Track"
		}

		// Read file from S3, log any errors
		rdr, err := awsBucket.GetReader(file.S3Path)
		if err != nil {
			switch t := err.(type) {
			case *s3.Error:
				if t.StatusCode == 404 {
					log.Printf("File not found. %s", file.S3Path)
				}
			default:
				log.Printf("Error downloading \"%s\" - %s", file.S3Path, err.Error())
			}
			continue
		}

		// Build a good path for the file within the zip
		zipPath := ""
		// Prefix playlist ID and name, if any
		if file.PlaylistID > 0 {
			zipPath += strconv.FormatInt(file.PlaylistID, 10) + "."
			// Build Safe Project Name
			file.PlaylistName = makeSafeFileName.ReplaceAllString(file.PlaylistName, "")
			if file.PlaylistName == "" { // Unlikely but just in case
				file.PlaylistName = "Playlist"
			}
			zipPath += file.PlaylistName + "/"
		}
		// Prefix folder name, if any
		if file.Folder != "" {
			zipPath += file.Folder
			if !strings.HasSuffix(zipPath, "/") {
				zipPath += "/"
			}
		}
		zipPath += safeFileName

		// We have to set a special flag so zip files recognize utf file names
		// See http://stackoverflow.com/questions/30026083/creating-a-zip-archive-with-unicode-filenames-using-gos-archive-zip
		h := &zip.FileHeader{
			Name:   zipPath,
			Method: zip.Deflate,
			Flags:  0x800,
		}

		f, _ := zipWriter.CreateHeader(h)

		io.Copy(f, rdr)
		rdr.Close()
	}

	zipWriter.Close()

	log.Printf("%s\t%s\t%s", r.Method, r.RequestURI, time.Since(start))
}
