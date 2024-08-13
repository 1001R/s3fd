package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Listing struct {
	client          *s3.Client
	Bucket          string
	keyMarker       *string
	versionIdMarker *string
	Count           int
	Done            bool
	MinKey          string
	MaxKey          string
}

func (l *Listing) Split(key string) *Listing {
	if l.keyMarker == nil || key < *l.keyMarker {
		panic("invalid split key")
	}
	newLister := &Listing{
		client:    l.client,
		Bucket:    l.Bucket,
		keyMarker: &key,
		MinKey:    key,
		MaxKey:    l.MaxKey,
	}
	l.MaxKey = key
	return newLister
}

func (l *Listing) SplitKey() (string, error) {
	return S3SplitKey(*l.keyMarker, l.MaxKey)
}

func NewListing(client *s3.Client, bucket, minKey, maxKey string) *Listing {
	return &Listing{
		client:    client,
		Bucket:    bucket,
		keyMarker: &minKey,
		MinKey:    minKey,
		MaxKey:    maxKey,
	}
}

func (l *Listing) NextPage(maxKeys int32) error {
	if l.Done {
		return nil
	}
	resp, err := l.client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
		Bucket:          &l.Bucket,
		KeyMarker:       l.keyMarker,
		VersionIdMarker: l.versionIdMarker,
		MaxKeys:         &maxKeys,
	})
	if err != nil {
		return err
	}
	timeFormat := "2006-01-02 15:04:05-07:00"
	for i := 0; i < len(resp.Versions); i++ {
		v := resp.Versions[i]
		if *v.Key < l.MaxKey {
			fmt.Printf("%s\t%s\t%d\t%s\n", *v.Key, *v.VersionId, *v.Size, v.LastModified.Format(timeFormat))
			l.Count++
		} else {
			l.Done = true
			break
		}
	}
	for i := 0; i < len(resp.DeleteMarkers); i++ {
		d := resp.DeleteMarkers[i]
		if *d.Key < l.MaxKey {
			fmt.Printf("%s\t%s\t-1\t%s\n", *d.Key, *d.VersionId, d.LastModified.Format(timeFormat))
			l.Count++
		} else {
			l.Done = true
			break
		}
	}
	if !l.Done && !*resp.IsTruncated {
		l.Done = true
	} else {
		l.keyMarker = resp.NextKeyMarker
		l.versionIdMarker = resp.NextVersionIdMarker
	}
	return nil
}
