package xyt

import (
	"slices"
	"sync"
	"time"

	"github.com/xyt-db/xyt/server"
)

// A Database is the top-level *thing* that xyt exposes.
// It contains operations around various Datasets, which are locations mapped
// to a a set of coordinates. These locations can be pre-allocated in such a
// way to avoid frequent re-allocations, and these locations can be sorted by
// time on insert to aid in querying.
//
// Database operations are thread-safe; all of the interesting stuff is either
// gated with mutexes (inserts, etc.), or are eventually consistent (updating
// internal metrics).
type Database struct {
	mutx sync.Mutex

	// data maps records as per:
	//   [record.Dataset][record.X][record.Y][]*record
	// Where the final slice contains references to records
	// sorted on their `When` value
	//
	// We don't really do much here with sharding or timestamps; certainly
	// not yet
	data map[string][][][]*server.Record

	// fields contains a union of the various fields a dataset contains
	fields map[string]map[string]interface{}

	// schemata maps the underlying ranges and sizes for a particular dataset
	schemata map[string]*server.Schema

	// stats holds varius dataset stats
	stats map[string]*Stats
}

// New creates a new Database and returns it for use and takes no tunables.
//
// Most of the fun stuff lives elsewhere, such as creating datasets.
func New() (d *Database, err error) {
	d = new(Database)
	d.data = make(map[string][][][]*server.Record)
	d.fields = make(map[string]map[string]interface{})
	d.schemata = make(map[string]*server.Schema)
	d.stats = make(map[string]*Stats)

	return
}

// Datasets returns a copy of all of the Dataset Schema known about in this database
//
// It purposefully returns clones of the underlying schemas, rather than the actual
// references to real schemata. The reason for this is twofold:
//
//  1. We want to gate against schemas being accidentally deallocated, which will
//     absolutely knacker all subsequent operations; and
//  2. We want to gate against developers trying to resize datasets by changing values
//     in this data. Xyt Databases don't work like that.
func (d *Database) Datasets() (ds map[string]*server.Schema) {
	// Make sure we're not passing about the actual, real schema or we
	// run the risk of being able to change values that end up breaking things,
	// like if someone decides to try and grow a dataset by changing the XMax
	// and/or the YMax value which just ends up breaking querying
	ds = make(map[string]*server.Schema)
	for k, v := range d.schemata {
		ds[k] = &server.Schema{
			Dataset:   v.Dataset,
			Frequency: v.Frequency,
			XMin:      v.XMin,
			XMax:      v.XMax,
			YMin:      v.YMin,
			YMax:      v.YMax,
		}
	}

	return
}

// Stats maps dataset names with things like record counts, and memory size
// for later reporting.
//
// This is handy data for, say, capacity planning
func (d *Database) Stats() map[string]*Stats {
	return d.stats
}

// CreateDataset takes a schema and pre-allocates a load of memory for that dataset.
//
// Schemas contain a number of handy tunables:
//
//	Frequency: we want to avoid having to grow/ reallocate data more than once per second;
//		   frequency allows us to limit this by growing the capacity of underlying data
//		   to cover the amount of data we reckon we'll insert in a second
//	SortOnInsert: when true, records are inserted into a dataset in ascending order, based
//		      on the `When` value. This greatly improves query time, but slows inserts
//		      on large datasets. The package benchmarks will show the actual effect
//	LazyInitialAllocate: for datasets where only a subset of locations are likely to be used,
//			     setting this to true can limit the amount of zero pages allocated
//			     to a xyt server, which is handy on systems with limited memory
//
// A sensible norm would be to set the frequency to 1 - 10hz, setting SortOnInsert to true, and
// LazyInitialAllocate to false; this will give you a nice, quick, trim dataset with good
// insert and query performance.
func (d *Database) CreateDataset(s *server.Schema) (err error) {
	err = d.validateSchema(s)
	if err != nil {
		return
	}

	if _, ok := d.data[s.Dataset]; ok {
		return DuplicateDatasetError
	}

	d.mutx.Lock()
	defer d.mutx.Unlock()

	d.schemata[s.Dataset] = s
	d.stats[s.Dataset] = newStats()

	d.data[s.Dataset] = make([][][]*server.Record, s.XMax-s.XMin)
	for xi := range d.data[s.Dataset] {
		d.data[s.Dataset][xi] = make([][]*server.Record, s.YMax-s.YMin)

		for yi := range d.data[s.Dataset][xi] {
			switch s.LazyInitialAllocate {
			case true:
				d.data[s.Dataset][xi][yi] = make([]*server.Record, 0)
			default:
				d.data[s.Dataset][xi][yi] = make([]*server.Record, 0, frequencyToSize(s.Frequency))
			}
		}
	}

	return
}

// InsertRecord takes a record, validates it for things like (X,Y) boundaries,
// required fields, and whether the underlying dataset exists, and then inserts
// it into the Database.
//
// This function will also grow underlying datasets according to the expected
// frequency in order to limit allocations.
//
// If the Dataset has been created with SortOnInsert=true, this function will also
// ensure data is stored in the correct order.
func (d *Database) InsertRecord(r *server.Record) (err error) {
	err = d.validateRecord(r)
	if err != nil {
		return
	}

	d.mutx.Lock()
	defer d.mutx.Unlock()

	// Ensure we have enough space allocated to avoid re-allocating on every write
	// and instead do allocations roughly once per second- which is at least more predictable
	schema := d.schemata[r.Dataset]

	if len(d.data[r.Dataset][r.X][r.Y]) >= cap(d.data[r.Dataset][r.X][r.Y]) {
		d.data[r.Dataset][r.X][r.Y] = slices.Grow(d.data[r.Dataset][r.X][r.Y], frequencyToSize(schema.Frequency))
	}

	d.data[r.Dataset][r.X][r.Y] = append(d.data[r.Dataset][r.X][r.Y], r)

	if schema.SortOnInsert {
		slices.SortFunc(d.data[r.Dataset][r.X][r.Y], func(a, b *server.Record) int {
			return a.Meta.When.AsTime().Compare(b.Meta.When.AsTime())
		})
	}

	if _, ok := d.fields[r.Dataset]; !ok {
		d.fields[r.Dataset] = make(map[string]interface{})
	}

	d.fields[r.Dataset][r.Name] = nil

	// Stats are eventually consistent
	go d.stats[r.Dataset].addRecord(r)

	return
}

// RetrieveRecords accepts a query and returns matching Records, erroing
// if the query is invalid.
func (d *Database) RetrieveRecords(q *server.Query) (r []*server.Record, err error) {
	if q == nil || q.Dataset == "" {
		return nil, MissingDatasetError
	}

	ds, ok := d.data[q.Dataset]
	if !ok {
		return nil, UnknownDatasetError
	}

	schema := d.schemata[q.Dataset]

	xMin, xMax := xRange(schema, q)
	yMin, yMax := yRange(schema, q)
	tMin, tMax, tAll := tRange(schema, q)

	timeStart, timeEnd, timeAll, timeLatest := timeRange(q)

	// If we only want the latest matching record, there's no real
	// need doing much beyond doing a backwards ranging of the data,
	// finding the first (ie: most recent) record matching the theta
	if timeLatest && !schema.SortOnInsert {
		err = UnsortedDataset

		return
	}

	r = make([]*server.Record, 0)

	for x := xMin; x < xMax; x++ {
		for y := yMin; y < yMax; y++ {
			if timeLatest {
				for ri := len(ds[x][y]) - 1; ri >= 0; ri-- {
					record := ds[x][y][ri]
					if !tAll {
						if record.T >= tMin && record.T < tMax {
							r = append(r, record)

							goto next
						}
					}
				}
			}

			for _, record := range ds[x][y] {
				if !timeAll {
					ts := record.Meta.When.AsTime()
					if ts.Before(timeStart) {
						continue
					}

					if ts.After(timeEnd) {
						if schema.SortOnInsert {
							goto next
						}

						continue
					}
				}

				if !tAll {
					if record.T < tMin || record.T >= tMax {
						continue
					}
				}

				r = append(r, record)
			}
		next:
		}

	}

	return
}

func (d *Database) validateRecord(r *server.Record) error {
	if r == nil {
		return EmptyRecordError
	}

	if r.Dataset == "" {
		return MissingDatasetError
	}

	if r.Name == "" {
		return MissingFieldNameError
	}

	schema, ok := d.schemata[r.Dataset]
	if !ok {
		return UnknownDatasetError
	}

	if r.X < schema.XMin || r.X > schema.XMax {
		return PositionOutOfBoundsError{
			dataset:  r.Dataset,
			position: positionX,
			min:      schema.XMin,
			max:      schema.XMax,
			received: r.X,
		}
	}

	if r.Y < schema.YMin || r.Y > schema.YMax {
		return PositionOutOfBoundsError{
			dataset:  r.Dataset,
			position: positionY,
			min:      schema.YMin,
			max:      schema.YMax,
			received: r.Y,
		}
	}

	if r.T < 0 || r.T > 360 {
		return PositionOutOfBoundsError{
			dataset:  r.Dataset,
			position: positionTheta,
			min:      0,
			max:      360,
			received: r.T,
		}
	}

	if r.Meta == nil || r.Meta.When.AsTime().IsZero() {
		return MissingWhenError
	}

	return nil
}

func (d *Database) validateSchema(s *server.Schema) error {
	if s == nil {
		return EmptySchemaError
	}

	if s.Dataset == "" {
		return MissingDatasetError
	}

	if s.XMin >= s.XMax {
		return InvalidCoordRangeError{s.Dataset, positionX, coordRangeErrorReasonMinMax}
	}

	if s.YMin >= s.YMax {
		return InvalidCoordRangeError{s.Dataset, positionY, coordRangeErrorReasonMinMax}
	}

	return nil
}

func frequencyToSize(f server.Frequency) int {
	switch f {
	case server.Frequency_F100Hz:
		return 100

	case server.Frequency_F1000Hz:
		return 1_000

	case server.Frequency_F10000Hz:
		return 10_000

	default:
		return 1
	}
}

func xRange(s *server.Schema, q *server.Query) (min, max int32) {
	switch v := q.X.(type) {
	case *server.Query_XAll:
		return s.XMin, s.XMax

	case *server.Query_XValue:
		return v.XValue, v.XValue + 1

	case *server.Query_XRange:
		return v.XRange.Start, v.XRange.End

	default:
		return s.XMin, s.XMax

	}
}

func yRange(s *server.Schema, q *server.Query) (min, max int32) {
	switch v := q.Y.(type) {
	case *server.Query_YAll:
		return s.YMin, s.YMax

	case *server.Query_YValue:
		return v.YValue, v.YValue + 1

	case *server.Query_YRange:
		return v.YRange.Start, v.YRange.End

	default:
		return s.YMin, s.YMax
	}
}

func tRange(_ *server.Schema, q *server.Query) (min, max int32, all bool) {
	switch v := q.T.(type) {
	case *server.Query_TAll:
		return 0, 360, true

	case *server.Query_TValue:
		return v.TValue, v.TValue + 1, false

	case *server.Query_TRange:
		return v.TRange.Start, v.TRange.End, false

	default:
		return 0, 360, all
	}
}

func timeRange(q *server.Query) (start, end time.Time, all, latest bool) {
	switch v := q.Time.(type) {
	case *server.Query_TimeRange:
		return v.TimeRange.Start.AsTime(), v.TimeRange.End.AsTime(), false, false

	case *server.Query_TimeLatest:
		latest = true

		return
	default:
		all = true

		return
	}
}
