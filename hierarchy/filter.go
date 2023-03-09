package hierarchy

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"

	aa_log "github.com/aaronland/go-log/v2"
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

	logger := log.Default()

	fn := func(ctx context.Context, r reader.Reader, parent_spr spr.StandardPlacesResult) (map[string]interface{}, error) {

		if parent_spr == nil {
			aa_log.Info(logger, "Parent SPR is nil, skipping")
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
// hood it invokes `ChoosePointInPolygonCandidateStrict` but return `nil` rather than an error if no matches are found.
func ChoosePointInPolygonCandidate(ctx context.Context, spatial_r reader.Reader, body []byte, possible []spr.StandardPlacesResult) (spr.StandardPlacesResult, error) {

	logger := log.Default()

	rsp, err := ChoosePointInPolygonCandidateStrict(ctx, spatial_r, body, possible)

	if err != nil {

		id_rsp := gjson.GetBytes(body, "properties.wof:id")

		aa_log.Warning(logger, "Failed to choose point in polygon candidate for '%d', %v\n", id_rsp.Int(), err)
		return nil, nil
	}

	return rsp, nil
}

// ChoosePointInPolygonCandidateStrict returns a SFO Museum specific whosonfirst/go-whosonfirst-spatial-hierarchy `FilterSPRResultsFunc` function
// for use with the whosonfirst/go-whosonfirst-spatial-hierarchy `PointInPolygonHierarchyResolver.PointInPolygonAndUpdate` method. It ensures that
// only a single match will be returned. It also ensures that all possible candidates have `sfomuseum:placetype` and `sfo:level` properties which
// match those found in 'body'. If those criteria can not be met it will return an error.
func ChoosePointInPolygonCandidateStrict(ctx context.Context, spatial_r reader.Reader, body []byte, possible []spr.StandardPlacesResult) (spr.StandardPlacesResult, error) {

	logger := log.Default()

	var parent_s spr.StandardPlacesResult
	count := len(possible)

	aa_log.Debug(logger, "Choose from %d results", count)

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

		// First cut of possible whose sfomuseum:placetype property matches pt
		candidates := make([]spr.StandardPlacesResult, 0)

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
					aa_log.Error(logger, "Failed to load %d, %v", p_id, err)
				} else {
					p_body = v
				}

				features.Store(p_id, v)
			}

			return p_body
		}

		spr_ch := make(chan spr.StandardPlacesResult)
		err_ch := make(chan error)
		done_ch := make(chan bool)

		for _, a := range ancestors {

			for _, r := range possible {

				go func(r spr.StandardPlacesResult) {

					defer func() {
						done_ch <- true
					}()

					p_id, err := strconv.ParseInt(r.Id(), 10, 64)

					if err != nil {
						err_ch <- fmt.Errorf("Failed to parse ID '%s', %w", r.Id(), err)
						return
					}

					p_body := load_feature(p_id)

					if p_body == nil {
						aa_log.Warning(logger, "Failed to load record for %d, skipping", p_id)
						return
					}

					pt_rsp := gjson.GetBytes(p_body, "properties.sfomuseum:placetype")

					if !pt_rsp.Exists() {
						err_ch <- fmt.Errorf("Record is missing sfomuseum:placetype property")
						return
					}

					if pt_rsp.String() == a.Name {
						aa_log.Debug(logger, "Placetype match for %s : %s (%d)", a.Name, r.Name(), p_id)
						spr_ch <- r
					}
				}(r)
			}

			remaining := len(possible)

			for remaining > 0 {
				select {
				case <-done_ch:
					remaining -= 1
				case err := <-err_ch:
					return nil, err
				case r := <-spr_ch:
					candidates = append(candidates, r)
				}
			}

			if len(candidates) > 0 {
				break
			}
		}

		aa_log.Debug(logger, "Candidate results after placetype filtering, %d", len(candidates))

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

			p_body := load_feature(p_id)

			if p_body == nil {
				return nil, fmt.Errorf("Failed to load feature for %d", p_id)
			}

			p_level_rsp := gjson.GetBytes(p_body, "properties.sfo:level")

			if !p_level_rsp.Exists() {
				aa_log.Warning(logger, "Record '%d' is missing sfo:level\n", p_id)
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
