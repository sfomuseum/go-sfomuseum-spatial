package spatial

import (
	"context"
	"github.com/whosonfirst/go-whosonfirst-iterate/iterator"
	"github.com/whosonfirst/go-whosonfirst-iterate/emitter"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	wof_feature "github.com/whosonfirst/go-whosonfirst-geojson-v2/feature"	
	"io"
	"fmt"
)

// NewSpatialDatabaseWithIterator returns a new `whosonfirst/go-whosonfirst-spatial` SpatialDatabase that has been populated
// by records emitted using a `whosonfirst/go-whosonfirst-iterate` instance, defined by `iterator_uri` and `iterator_sources`.
func NewSpatialDatabaseWithIterator(ctx context.Context, spatial_database_uri string, iterator_uri string, iterator_sources ...string) (database.SpatialDatabase, error) {

	spatial_db, err := database.NewSpatialDatabase(ctx, spatial_database_uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to create new spatial database, %w", err)
	}

	err = PopulateSpatialDatabaseWithIterator(ctx, spatial_db, iterator_uri, iterator_sources...)

	if err != nil {
		return nil, fmt.Errorf("Failed to populate spatial database, %w", err)
	}

	return spatial_db, nil
}

// Populate an existing `whosonfirst/go-whosonfirst-spatial` SpatialDatabase instance with by records emitted using a
// `whosonfirst/go-whosonfirst-iterate` instance, defined by `iterator_uri` and `iterator_sources`.
func PopulateSpatialDatabaseWithIterator(ctx context.Context, spatial_db database.SpatialDatabase, iterator_uri string, iterator_sources ...string) error {


	iter_cb := func(ctx context.Context, fh io.ReadSeeker, args ...interface{}) error {

		path, err := emitter.PathForContext(ctx)

		if err != nil {
			return fmt.Errorf("Failed to derive path for context, %w", err)
		}

		f, err := wof_feature.LoadFeatureFromReader(fh)

		if err != nil {
			return fmt.Errorf("Failed to load GeoJSON feature for %s, %w", path, err)
		}

		err = spatial_db.IndexFeature(ctx, f)

		if err != nil {
			return fmt.Errorf("Failed to index feature for %s, %w", path, err)
		}

		return nil
	}

	iter, err := iterator.NewIterator(ctx, iterator_uri, iter_cb)

	if err != nil {
		return fmt.Errorf("Failed to create new iterator, %w", err)
	}

	err = iter.IterateURIs(ctx, iterator_sources...)

	if err != nil {
		return fmt.Errorf("Failed to iterate sources, %w", err)
	}

	return nil
}
