package hierarchy

import (
	"context"
	"fmt"
	"log"
	"strconv"

	sfom_placetypes "github.com/sfomuseum/go-sfomuseum-placetypes"
	sfom_reader "github.com/sfomuseum/go-sfomuseum-reader"
	"github.com/tidwall/gjson"
	"github.com/whosonfirst/go-reader"
	"github.com/whosonfirst/go-whosonfirst-feature/properties"
	wof_placetypes "github.com/whosonfirst/go-whosonfirst-placetypes"
	"github.com/whosonfirst/go-whosonfirst-spatial-hierarchy"
	"github.com/whosonfirst/go-whosonfirst-spr/v2"
)

// DefaultPointInPolygonToolUpdateCallback returns a SFO Museum specific whosonfirst/go-whosonfirst-spatial-hierarchy `PointInPolygonHierarchyResolverUpdateCallback`
// function for use with the whosonfirst/go-whosonfirst-spatial-hierarchy `PointInPolygonHierarchyResolver.PointInPolygonAndUpdate` method.
func DefaultPointInPolygonToolUpdateCallback() hierarchy.PointInPolygonHierarchyResolverUpdateCallback {

	fn := func(ctx context.Context, r reader.Reader, parent_spr spr.StandardPlacesResult) (map[string]interface{}, error) {

		if parent_spr == nil {
			log.Println("SKIP")
			return nil, nil
		}

		to_update := make(map[string]interface{})

		parent_id, err := strconv.ParseInt(parent_spr.Id(), 10, 64)

		if err != nil {
			return nil, err
		}

		parent_f, err := sfom_reader.LoadBytesFromID(ctx, r, parent_id)

		if err != nil {
			return nil, err
		}

		parent_hierarchy := properties.Hierarchies(parent_f)
		parent_country := properties.Country(parent_f)

		to_update = map[string]interface{}{
			"properties.wof:parent_id": parent_id,
			"properties.wof:country":   parent_country,
			"properties.wof:hierarchy": parent_hierarchy,
		}

		return to_update, nil
	}

	return fn
}

// ChoosePointInPolygonCandidate returns a SFO Museum specific whosonfirst/go-whosonfirst-spatial-hierarchy `FilterSPRResultsFunc` function
// for use with the whosonfirst/go-whosonfirst-spatial-hierarchy `PointInPolygonHierarchyResolver.PointInPolygonAndUpdate` method. Under the
// hood it invokes `ChoosePointInPolygonCandidateStrict` but return `nil` rather than an error if no matches are found.
func ChoosePointInPolygonCandidate(ctx context.Context, spatial_r reader.Reader, body []byte, possible []spr.StandardPlacesResult) (spr.StandardPlacesResult, error) {

	rsp, err := ChoosePointInPolygonCandidateStrict(ctx, spatial_r, body, possible)

	if err != nil {

		id_rsp := gjson.GetBytes(body, "properties.wof:id")

		log.Printf("Failed to choose point in polygon candidate for '%d', %v\n", id_rsp.Int(), err)
		return nil, nil
	}

	return rsp, nil
}

// ChoosePointInPolygonCandidateStrict returns a SFO Museum specific whosonfirst/go-whosonfirst-spatial-hierarchy `FilterSPRResultsFunc` function
// for use with the whosonfirst/go-whosonfirst-spatial-hierarchy `PointInPolygonHierarchyResolver.PointInPolygonAndUpdate` method. It ensures that
// only a single match will be returned. If that criteria can not be met it will return an error.
func ChoosePointInPolygonCandidateStrict(ctx context.Context, spatial_r reader.Reader, body []byte, possible []spr.StandardPlacesResult) (spr.StandardPlacesResult, error) {

	var parent_s spr.StandardPlacesResult
	count := len(possible)

	switch count {
	case 0:

		return nil, fmt.Errorf("No results")

	case 1:

		parent_s = possible[0]

	default:

		// START OF sfomuseum:placetype stuff

		pt_rsp := gjson.GetBytes(body, "properties.sfomuseum:placetype")

		if !pt_rsp.Exists() {
			return nil, fmt.Errorf("Record is missing sfomuseum:placetype property")
		}

		pt_spec, err := sfom_placetypes.SFOMuseumPlacetypeSpecification()

		if err != nil {
			return nil, fmt.Errorf("Failed to load SFO Museum placetype specification, %w", err)
		}

		pt, err := pt_spec.GetPlacetypeByName(pt_rsp.String())

		if err != nil {
			return nil, fmt.Errorf("Failed to load placetype '%s', %w", pt_rsp.String(), err)
		}

		roles := wof_placetypes.AllRoles()

		ancestors := pt_spec.AncestorsForRoles(pt, roles)

		candidates := make([]spr.StandardPlacesResult, 0)

		for _, a := range ancestors {

			for _, r := range possible {

				p_id, err := strconv.ParseInt(r.Id(), 10, 64)

				if err != nil {
					return nil, err
				}

				p_body, err := sfom_reader.LoadBytesFromID(ctx, spatial_r, p_id)

				if err != nil {
					return nil, err
				}

				pt_rsp := gjson.GetBytes(p_body, "properties.sfo:placetype")

				if pt_rsp.String() == a.Name {
					candidates = append(candidates, r)
				}
			}

			if len(candidates) > 0 {
				break
			}
		}

		// END OF sfomuseum:placetype stuff

		filtered := make([]spr.StandardPlacesResult, 0)

		level_rsp := gjson.GetBytes(body, "properties.sfo:level")

		if !level_rsp.Exists() {
			return nil, fmt.Errorf("Record is missing sfo:level\n")
		}

		f_level := level_rsp.Int()

		for _, r := range candidates {

			// log.Printf("%s (%d) %s (%s) %s\n", work.Name, work.ObjectID, r.Name(), r.Id(), r.Placetype())

			p_id, err := strconv.ParseInt(r.Id(), 10, 64)

			if err != nil {
				return nil, err
			}

			p_body, err := sfom_reader.LoadBytesFromID(ctx, spatial_r, p_id)

			if err != nil {
				return nil, err
			}

			p_level_rsp := gjson.GetBytes(p_body, "properties.sfo:level")

			if !p_level_rsp.Exists() {
				log.Printf("Record '%d' is missing sfo:level\n", p_id)
				continue
			}

			p_level := p_level_rsp.Int()

			if p_level != f_level {
				continue
			}

			filtered = append(filtered, r)
		}

		count := len(filtered)

		switch count {
		case 0:

			return nil, fmt.Errorf("No results, after filtering")
		case 1:
			parent_s = filtered[0]
		default:

			for _, s := range filtered {
				log.Println(s.Name(), s.Id())
			}

			return nil, fmt.Errorf("Multiple results (%d), after filtering", count)
		}
	}

	return parent_s, nil
}
