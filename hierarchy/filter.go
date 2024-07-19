package hierarchy

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync"

	sfom_placetypes "github.com/sfomuseum/go-sfomuseum-placetypes"
	sfom_reader "github.com/sfomuseum/go-sfomuseum-reader"
	"github.com/tidwall/gjson"
	"github.com/whosonfirst/go-reader"
	"github.com/whosonfirst/go-whosonfirst-feature/properties"
	wof_placetypes "github.com/whosonfirst/go-whosonfirst-placetypes"
	"github.com/whosonfirst/go-whosonfirst-spatial/hierarchy"
	"github.com/whosonfirst/go-whosonfirst-spr/v2"
)

// DefaultPointInPolygonToolUpdateCallback returns a SFO Museum specific whosonfirst/go-whosonfirst-spatial-hierarchy `PointInPolygonHierarchyResolverUpdateCallback`
// function for use with the whosonfirst/go-whosonfirst-spatial-hierarchy `PointInPolygonHierarchyResolver.PointInPolygonAndUpdate` method.
func DefaultPointInPolygonToolUpdateCallback() hierarchy.PointInPolygonHierarchyResolverUpdateCallback {

	fn := func(ctx context.Context, r reader.Reader, parent_spr spr.StandardPlacesResult) (map[string]interface{}, error) {

		if parent_spr == nil {
			slog.Debug("Parent SPR is nil, skipping")
			return nil, nil
		}

		to_update := make(map[string]interface{})

		parent_id, err := strconv.ParseInt(parent_spr.Id(), 10, 64)

		if err != nil {
			return nil, fmt.Errorf("Failed to parse parent SPR ID '%s', %w", parent_spr.Id(), err)
		}

		parent_f, err := sfom_reader.LoadBytesFromID(ctx, r, parent_id)

		if err != nil {
			return nil, fmt.Errorf("Failed to load parent ID %d, %w", parent_id, err)
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
// hood it invokes `ChoosePointInPolygonCandidateStrict` but returns `nil` rather than an error if no matches are found.
func ChoosePointInPolygonCandidate(ctx context.Context, spatial_r reader.Reader, body []byte, possible []spr.StandardPlacesResult) (spr.StandardPlacesResult, error) {

	rsp, err := ChoosePointInPolygonCandidateStrict(ctx, spatial_r, body, possible)

	if err != nil {
		id_rsp := gjson.GetBytes(body, "properties.wof:id")
		slog.Warn("Failed to choose point in polygon candidate", "id", id_rsp.Int(), "error", err)
		return nil, nil
	}

	return rsp, nil
}

// ChoosePointInPolygonCandidateStrict returns a SFO Museum specific whosonfirst/go-whosonfirst-spatial-hierarchy `FilterSPRResultsFunc` function
// for use with the whosonfirst/go-whosonfirst-spatial-hierarchy `PointInPolygonHierarchyResolver.PointInPolygonAndUpdate` method. It ensures that
// only a single match will be returned. It also ensures that all possible candidates have `sfomuseum:placetype` and `sfo:level` properties which
// match those found in 'body'. If those criteria can not be met it will return an error.
func ChoosePointInPolygonCandidateStrict(ctx context.Context, spatial_r reader.Reader, body []byte, possible []spr.StandardPlacesResult) (spr.StandardPlacesResult, error) {

	id_rsp := gjson.GetBytes(body, "properties.wof:id")
	id := id_rsp.String()

	name_rsp := gjson.GetBytes(body, "properties.wof:name")
	name := name_rsp.String()

	logger := slog.Default()
	logger = logger.With("id", id)
	logger = logger.With("name", name)

	var parent_s spr.StandardPlacesResult
	count := len(possible)

	logger.Debug("Choose from candidate results BEFORE filtering", "count", count)

	switch count {
	case 0:
		return nil, fmt.Errorf("No results")
	// case 1:
	//	parent_s = possible[0]
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

		logger = logger.With("placetype", pt_rsp.String())

		level_rsp := gjson.GetBytes(body, "properties.sfo:level")

		if !level_rsp.Exists() {
			return nil, fmt.Errorf("Record is missing sfo:level\n")
		}

		f_level := level_rsp.Int()

		logger = logger.With("level", f_level)

		// Get ancestors

		roles := wof_placetypes.AllRoles()
		ancestors := pt_spec.AncestorsForRoles(pt, roles)

		// logger.Debug("Ancestors", "roles", roles, "ancestors", ancestors)

		// Local cache of features we need to fetch in order to inspect non-SPR
		// properties

		features := new(sync.Map)

		// Local function for fetching/cache features

		load_feature := func(p_id int64) []byte {

			var p_body []byte

			v, exists := features.Load(p_id)

			if exists {
				p_body = v.([]byte)
			} else {

				v, err := sfom_reader.LoadBytesFromID(ctx, spatial_r, p_id)

				if err != nil {
					logger.Error("Failed to load parent record", "parent id", p_id, "error", err)
				} else {
					p_body = v
				}

				features.Store(p_id, v)
			}

			return p_body
		}

		for _, p := range possible {
			logger.Debug("POSSIBLE", "id", p.Id(), "name", p.Name(), "placetype", p.Placetype())
		}

		for _, a := range ancestors {
			logger.Debug("ANCESTOR", "name", a.Name)
		}

		for _, a := range ancestors {

			// logger.Debug("Process ancestor (compare to possible)", "ancestor", a, "offset", idx)

			for _, candidate := range possible {

				logger.Debug("Compare placetype for candidate", "ancestor", a.Name, "candidate id", candidate.Id())

				candidate_id, err := strconv.ParseInt(candidate.Id(), 10, 64)

				if err != nil {
					return nil, fmt.Errorf("Failed to parse candidate ID '%s', %w", candidate.Id(), err)
				}

				candidate_body := load_feature(candidate_id)

				if candidate_body == nil {
					logger.Warn("Failed to load record, skipping", "candidate id", candidate_id)
					continue
				}

				// Placetype check(s)

				pt_rsp := gjson.GetBytes(candidate_body, "properties.sfomuseum:placetype")

				if !pt_rsp.Exists() {
					return nil, fmt.Errorf("Record is missing sfomuseum:placetype property")
				}

				candidate_pt := pt_rsp.String()

				pt_match := candidate_pt == a.Name

				logger.Debug("Placetype check", "candidate id", candidate_id, "candidate placetype", candidate_pt, "ancestor", a.Name, "match", pt_match)

				if !pt_match {
					continue
				}

				// Level check(s)

				skip_level_checks := []string{
					"building",
					"hotel",
					"garage",
					"rail",
					"campus",
					"airport",
				}

				if !slices.Contains(skip_level_checks, candidate_pt) {

					c_level_rsp := gjson.GetBytes(candidate_body, "properties.sfo:level")

					if !c_level_rsp.Exists() {
						logger.Warn("Record is missing sfo:level", "candidate id", candidate_id)
						continue
					}

					candidate_level := c_level_rsp.Int()

					level_match := candidate_level == f_level

					logger.Debug("sfo:level check", "candidate", candidate_id, "candidate level", candidate_level, "feature level", f_level, "match", level_match)

					if !level_match {
						continue
					}
				}

				parent_s = candidate
				break
			}
		}

		if parent_s == nil {
			return nil, fmt.Errorf("Unable to derive parent record")
		}

		logger.Debug("OKAY", "parent", parent_s.Id(), "name", parent_s.Name())
		return parent_s, nil
	}
}
