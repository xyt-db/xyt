package xyt

import (
	"slices"
	"sync"
	"time"

	"github.com/xyt-db/xyt/server"
)

// A Database provides access to various xyt stuff
type Database struct {
	mutx sync.Mutex

	// data maps records as per:
	//   [record.Dataset][record.X][record.Y][record.T][*record]
	// Where the final slice contains references to records
	// sorted on their `When` value
	//
	// We don't really do much here with sharding or timestamps; certainly
	// not yet
	data map[string][][][][]*server.Record

	// fields contains a union of the various fields a dataset contains
	fields map[string]map[string]interface{}

	// schemata maps the underlying ranges and sizes for a particular dataset
	schemata map[string]*server.Schema
}

// New creates a new Database and returns it for use
func New() (d *Database, err error) {
	d = new(Database)
	d.data = make(map[string][][][][]*server.Record)
	d.fields = make(map[string]map[string]interface{})
	d.schemata = make(map[string]*server.Schema)

	return
}

// CreateDataset takes a schema and creates the underlying data
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

	d.data[s.Dataset] = make([][][][]*server.Record, s.XMax-s.XMin)
	for xi := range d.data[s.Dataset] {
		d.data[s.Dataset][xi] = make([][][]*server.Record, s.YMax-s.YMin)

		for yi := range d.data[s.Dataset][xi] {
			d.data[s.Dataset][xi][yi] = make([][]*server.Record, 360)

			for ti := range d.data[s.Dataset][xi][yi] {
				d.data[s.Dataset][xi][yi][ti] = make([]*server.Record, 0, frequencyToSize(s.Frequency))
			}
		}
	}

	return
}

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

	if len(d.data[r.Dataset][r.X][r.Y][r.T]) == cap(d.data[r.Dataset][r.X][r.Y][r.T]) {
		d.data[r.Dataset][r.X][r.Y][r.T] = slices.Grow(d.data[r.Dataset][r.X][r.Y][r.T], frequencyToSize(schema.Frequency))
	}

	d.data[r.Dataset][r.X][r.Y][r.T] = append(d.data[r.Dataset][r.X][r.Y][r.T], r)

	if schema.SortOnInsert {
		slices.SortFunc(d.data[r.Dataset][r.X][r.Y][r.T], func(a, b *server.Record) int {
			return a.Meta.When.AsTime().Compare(b.Meta.When.AsTime())
		})
	}

	if _, ok := d.fields[r.Dataset]; !ok {
		d.fields[r.Dataset] = make(map[string]interface{})
	}

	d.fields[r.Dataset][r.Name] = nil

	return
}

func (d *Database) RetrieveRecords(q *server.Query) (r []*server.Record, err error) {
	if q == nil || q.Dataset == "" {
		return nil, MissingDatasetError
	}

	ds, ok := d.data[q.Dataset]
	if !ok {
		return nil, UnknownDatasetError
	}

	start := time.Time{}
	end := time.Now()

	if q.From.CheckValid() == nil {
		start = q.From.AsTime()
	}

	if q.Until.CheckValid() == nil {
		end = q.Until.AsTime()
	}

	schema := d.schemata[q.Dataset]

	xMin := q.XMin
	xMax := q.XMax
	if xMax == 0 {
		xMax = schema.XMax
	}

	yMin := q.YMin
	yMax := q.YMax
	if yMax == 0 {
		yMax = schema.YMax
	}

	tMin := q.TMin
	tMax := q.TMax
	if tMax == 0 {
		tMax = 360
	}

	r = make([]*server.Record, 0)

	for x := xMin; x < xMax; x++ {
		for y := yMin; y < yMax; y++ {
			for t := tMin; t < tMax; t++ {
				for _, record := range ds[x][y][t] {
					ts := record.Meta.When.AsTime()
					if ts.Before(start) {
						continue
					}

					if ts.After(end) {
						if schema.SortOnInsert {
							goto next
						}

						continue
					}

					r = append(r, record)
				}
			next:
			}
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
			position: "X",
			min:      schema.XMin,
			max:      schema.XMax,
			received: r.X,
		}
	}

	if r.Y < schema.YMin || r.Y > schema.YMax {
		return PositionOutOfBoundsError{
			dataset:  r.Dataset,
			position: "Y",
			min:      schema.YMin,
			max:      schema.YMax,
			received: r.Y,
		}
	}

	if r.T < 0 || r.T > 360 {
		return PositionOutOfBoundsError{
			dataset:  r.Dataset,
			position: "T",
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
		return InvalidCoordRangeError{s.Dataset, "X", "XMin must be less than XMax"}
	}

	if s.YMin >= s.YMax {
		return InvalidCoordRangeError{s.Dataset, "X", "YMin must be less than YMax"}
	}

	return nil
}

func frequencyToSize(f server.Frequency) int {
	switch f {
	case server.Frequency_F1Hz:
		return 1

	case server.Frequency_F100Hz:
		return 100

	case server.Frequency_F1000Hz:
		return 1_000

	case server.Frequency_F10000Hz:
		return 10_000
	}

	return 1
}
