package feature

import (
	"github.com/whosonfirst/go-whosonfirst-geojson-v2"
	wof_feature "github.com/whosonfirst/go-whosonfirst-geojson-v2/feature"
	"io"
)

func LoadFeature(body []byte) (geojson.Feature, error) {

	return wof_feature.NewGeoJSONFeature(body)
}

func LoadFeatureFromReader(fh io.Reader) (geojson.Feature, error) {

	body, err := wof_feature.UnmarshalFeatureFromReader(fh)

	if err != nil {
		return nil, err
	}

	return LoadFeature(body)
}

func LoadFeatureFromFile(path string) (geojson.Feature, error) {

	body, err := wof_feature.UnmarshalFeatureFromFile(path)

	if err != nil {
		return nil, err
	}

	return LoadFeature(body)
}
