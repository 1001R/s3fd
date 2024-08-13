package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)
	fBucket := flag.String("bucket", "", "Bucket")
	// fMinSize := flag.Int64("minsize", 0, "Minimum object size")
	// fMaxSize := flag.Int64("maxsize", -1, "Maximum object size")
	fNumWorkers := flag.Int("workers", 4, "Number of worker threads")
	flag.Parse()
	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("eu-west-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	s3Client := s3.NewFromConfig(cfg)
	listings := make(chan *Listing)
	split := make(chan struct{}, *fNumWorkers)
	pending := make(chan int, *fNumWorkers)

	for i := 0; i < *fNumWorkers; i++ {
		go Worker(i, listings, split, pending)
	}

	numChars := int(MAX_KEY - MIN_KEY)
	wsize := numChars / *fNumWorkers
	rem := numChars - wsize**fNumWorkers
	minRune := MIN_KEY
	for i := 0; i < *fNumWorkers; i++ {
		minKey := string(utf8.AppendRune(nil, minRune))
		maxRune := minRune + rune(wsize)
		if *fNumWorkers-i <= rem {
			maxRune += 1
		}
		maxKey := string(utf8.AppendRune(nil, maxRune))
		pending <- 1
		listings <- NewListing(s3Client, *fBucket, minKey, maxKey)
		minRune = maxRune
	}

	numPending := 0
	for i := range pending {
		numPending += i
		slog.Info("number of pending jobs changed", "jobs", numPending)
		if numPending == 0 {
			break
		}
	}
	close(listings)
}
