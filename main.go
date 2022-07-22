package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/customerio/homework/datastore"
	"github.com/customerio/homework/serve"
	"github.com/customerio/homework/stream"
)

var (
	input = flag.String("input", "", "data file to run reports against")
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	flag.Parse()

	if *input == "" {
		log.Fatal("must specify --input")
	}

	f, err := os.Open(*input)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	data := datastore.Datastore{
		Customers: make(map[int]*serve.Customer),
	}

	if ch, err := stream.Process(ctx, f); err == nil {
		for rec := range ch {
			if rec.UserID == "" {
				continue
			}

			id, _ := strconv.Atoi(rec.UserID)
			switch rec.Type {
			case "attributes":
				rec.Data["timestamp"] = strconv.Itoa(int(rec.Timestamp))
				data.Update(id, rec.Data)
			case "event":
				data.GroupEventsByNamePerUser(id, rec)
			}
		}
		if err := ctx.Err(); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Println("stream processing failed, maybe you need to implement it?", err)
	}

	if err := serve.ListenAndServe(":1323", data); err != nil {
		log.Fatal(err)
	}
}
