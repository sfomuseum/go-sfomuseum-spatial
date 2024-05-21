package main

import (
	"context"
	"log"

	_ "github.com/whosonfirst/go-whosonfirst-spatial-pmtiles"
	"github.com/whosonfirst/go-whosonfirst-spatial/app/hierarchy/update"
	"github.com/sfomuseum/go-sfomuseum-spatial/hierarchy"	
)

func main() {

	ctx := context.Background()
	logger := log.Default()

	fs, err := update.DefaultFlagSet(ctx)

	if err != nil {
		log.Fatalf("Failed to create default flagset, %v", err)
	}
	
	opts, err := update.RunOptionsFromFlagSet(ctx, fs)

	if err != nil {
		log.Fatalf("Failed to derive options from flagset, %v", err)
	}

	update_func := hierarchy.DefaultPointInPolygonToolUpdateCallback()
	
	opts.SPRResultsFunc = hierarchy.ChoosePointInPolygonCandidate
	opts.PIPUpdateFunc = update_func
	
	err = update.RunWithOptions(ctx, opts, logger)

	if err != nil {
		log.Fatalf("Failed to run update hierarchy tool, %v", err)
	}
}
