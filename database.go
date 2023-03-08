package spatial

import (
	"context"
	"fmt"

	"github.com/whosonfirst/go-whosonfirst-spatial/database"
)

// NewSpatialDatabaseWithIterator returns a new `whosonfirst/go-whosonfirst-spatial` SpatialDatabase that has been populated
// by records emitted using a `whosonfirst/go-whosonfirst-iterate` instance, defined by `iterator_uri` and `iterator_sources`.
func NewSpatialDatabaseWithIterator(ctx context.Context, spatial_database_uri string, iterator_uri string, iterator_sources ...string) (database.SpatialDatabase, error) {

	spatial_db, err := database.NewSpatialDatabase(ctx, spatial_database_uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to create new spatial database, %w", err)
	}

	err = database.IndexDatabaseWithIterator(ctx, spatial_db, iterator_uri, iterator_sources...)

	if err != nil {
		return nil, fmt.Errorf("Failed to populate spatial database, %w", err)
	}

	return spatial_db, nil
}
