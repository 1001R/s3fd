package main

import (
	"log/slog"
)

func Worker(i int, listings chan *Listing, split chan struct{}, done chan int) {
	for l := range listings {
		logger := slog.Default().With("worker", i, "minKey", l.MinKey, "maxKey", l.MaxKey)
		logger.Info("start")
		var maxKeys int32 = 10
		for {
			if err := l.NextPage(maxKeys); err != nil {
				panic(err)
			}
			if l.Done {
				break
			}
			splitKey, err := l.SplitKey()
			if err != nil {
				panic(err)
			}
			if splitKey != "" {
				select {
				case <-split:
					logger.Info("split", "splitKey", splitKey)
					done <- 1
					listings <- l.Split(splitKey)
				default:
					// no need to split, continue
				}
			} else {
				logger.Info("cannot split")
			}
			maxKeys = 1000
		}
		logger.Info("done", "count", l.Count)
		// let others know that we need more work
		split <- struct{}{}
		done <- -1
	}
	slog.Info("worker terminated", "worker", i)
}
