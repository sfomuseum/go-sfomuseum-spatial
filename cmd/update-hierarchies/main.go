package main

import (
	"context"
	"log"

	_ "github.com/whosonfirst/go-whosonfirst-spatial-pmtiles"
	_ "github.com/whosonfirst/go-whosonfirst-spatial-sqlite"	
	
	sfom_hierarchy "github.com/sfomuseum/go-sfomuseum-spatial/hierarchy"
	wof_update "github.com/whosonfirst/go-whosonfirst-spatial/app/hierarchy/update"
)

func main() {

	ctx := context.Background()

	fs, err := wof_update.DefaultFlagSet(ctx)

	if err != nil {
		log.Fatalf("Failed to create default flagset, %v", err)
	}

	opts, err := wof_update.RunOptionsFromFlagSet(ctx, fs)

	if err != nil {
		log.Fatalf("Failed to derive options from flagset, %v", err)
	}

	sfom_results_func := sfom_hierarchy.ChoosePointInPolygonCandidate
	sfom_update_func := sfom_hierarchy.DefaultPointInPolygonToolUpdateCallback()

	opts.SPRResultsFunc = sfom_results_func
	opts.PIPUpdateFunc = sfom_update_func

 	err = wof_update.RunWithOptions(ctx, opts)

	if err != nil {
		log.Fatalf("Failed to run update hierarchy tool, %v", err)
	}
}
