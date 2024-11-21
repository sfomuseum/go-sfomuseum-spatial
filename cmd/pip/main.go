package main

import (
	"context"
	"log"

	_ "github.com/whosonfirst/go-whosonfirst-spatial-pmtiles"
	_ "github.com/whosonfirst/go-whosonfirst-spatial-sqlite"	
	
	"github.com/whosonfirst/go-whosonfirst-spatial/app/pip"
)

func main() {

	ctx := context.Background()
	err := pip.Run(ctx)

	if err != nil {
		log.Fatalf("Failed to run update hierarchy tool, %v", err)
	}
}
