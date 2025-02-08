package xyt

import (
	"math/rand"
	"testing"
	"time"

	"github.com/xyt-db/xyt/server"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// rseed is used to seed the random generator; we want the same seed
// each time, ideally, so we can nvestigate errors
const rseed int64 = 2144610006738168

var (
	r     = rand.New(rand.NewSource(rseed))
	chars = []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '.', ','}
)

func TestDatabase_Add(t *testing.T) {
	for _, test := range []struct {
		name        string
		record      *server.Record
		expectError bool
	}{
		{"Nil record fails", nil, true},
		{"Empty dataset fails", &server.Record{}, true},
		{"Empty Name fails", &server.Record{Dataset: "site-a", X: 1, Y: 1, T: 90}, true},
		{"Empty When fails", &server.Record{Dataset: "site-a", X: 1, Y: 1, T: 90, Name: "temperature"}, true},
		{"Too low X value fails", &server.Record{Dataset: "site-a", X: -11, Y: 1, T: 90, Name: "temperature", Meta: &server.Metadata{When: timestamppb.New(time.Now())}}, true},
		{"Too high X value fails", &server.Record{Dataset: "site-a", X: 11, Y: 1, T: 90, Name: "temperature", Meta: &server.Metadata{When: timestamppb.New(time.Now())}}, true},
		{"Too low Y value fails", &server.Record{Dataset: "site-a", X: 1, Y: -11, T: 90, Name: "temperature", Meta: &server.Metadata{When: timestamppb.New(time.Now())}}, true},
		{"Too high Y value fails", &server.Record{Dataset: "site-a", X: 1, Y: 11, T: 90, Name: "temperature", Meta: &server.Metadata{When: timestamppb.New(time.Now())}}, true},
		{"Too low T value fails", &server.Record{Dataset: "site-a", X: 1, Y: 1, T: -1000, Name: "temperature", Meta: &server.Metadata{When: timestamppb.New(time.Now())}}, true},
		{"Too high T value fails", &server.Record{Dataset: "site-a", X: 1, Y: 1, T: 1000, Name: "temperature", Meta: &server.Metadata{When: timestamppb.New(time.Now())}}, true},
		{"Unknown dataset errors", &server.Record{Dataset: "site-b", X: 1, Y: 1, T: 90, Name: "temperature", Meta: &server.Metadata{When: timestamppb.New(time.Now())}}, true},

		{"Happy path", &server.Record{Dataset: "site-a", X: 1, Y: 1, T: 90, Name: "temperature", Meta: &server.Metadata{When: timestamppb.Now()}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			d, err := New()
			if err != nil {
				// If this fails then there's no'ope
				t.Fatal(err)
			}

			err = d.CreateDataset(&server.Schema{
				Dataset:      "site-a",
				XMin:         0,
				XMax:         10,
				YMin:         0,
				YMax:         10,
				Frequency:    server.Frequency_F10000Hz,
				SortOnInsert: true,
			})

			err = d.InsertRecord(test.record)
			if err == nil && test.expectError {
				t.Errorf("expected error, received none")
			} else if err != nil && !test.expectError {
				t.Errorf("unexpected error %#v", err)
			}
		})
	}
}

func TestDatabase_CreateDataset(t *testing.T) {
	d, err := New()
	if err != nil {
		// If this fails then there's no'ope
		t.Fatal(err)
	}

	// Create an initial dataset so we can test dulpicate dataset names
	// fail
	d.CreateDataset(&server.Schema{
		Dataset:   "site-a",
		XMin:      0,
		XMax:      10,
		YMin:      0,
		YMax:      10,
		Frequency: server.Frequency_F100Hz,
	})

	for _, test := range []struct {
		name        string
		schema      *server.Schema
		expectError bool
	}{
		{"Duplicate dataset names fail", &server.Schema{Dataset: "site-a", XMax: 10, YMax: 10}, true},
		{"Nil schema fails", nil, true},
		{"Empty dataset name fails", &server.Schema{}, true},
		{"Unset XMax fails", &server.Schema{Dataset: "racecourse"}, true},
		{"Unset YMax fails", &server.Schema{Dataset: "racecourse", XMax: 10}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := d.CreateDataset(test.schema)
			if err == nil && test.expectError {
				t.Errorf("expected error, received none")
			} else if err != nil && !test.expectError {
				t.Errorf("unexpected error %#v", err)
			}
		})
	}
}

func TestDatabase_RetrieveRecords(t *testing.T) {
	// Start is useful for later query tests
	start := time.Now()

	d, err := New()
	if err != nil {
		// If this fails then there's no'ope
		t.Fatal(err)
	}

	// Create some data to query
	err = d.CreateDataset(&server.Schema{
		Dataset: "site-a",
		XMin:    0,
		XMax:    10,
		YMin:    0,
		YMax:    10,
	})
	if err != nil {
		// If this fails then there's no'ope
		t.Fatal(err)
	}

	for x := int32(0); x < 10; x++ {
		for y := int32(0); y < 10; y++ {
			err = d.InsertRecord(&server.Record{
				Meta: &server.Metadata{
					When: timestamppb.Now(),
					Labels: map[string]string{
						"hello": "world",
					},
				},
				Dataset: "site-a",
				Name:    "Bloops",
				Value:   1000,
				X:       x,
				Y:       y,
				T:       90,
			})
			if err != nil {
				// If this fails then there's no'ope
				t.Fatal(err)
			}

		}
	}

	for _, test := range []struct {
		name        string
		query       *server.Query
		expectCount int
		expectError bool
	}{
		{"Nil Query errors", nil, 0, true},
		{"Missing Dataset errors", &server.Query{}, 0, true},
		{"Unknown Dataset errors", &server.Query{Dataset: "site-b"}, 0, true},
		{"Empty query returns all data", &server.Query{Dataset: "site-a"}, 100, false},
		{"Setting end after time after the start returns nothing", &server.Query{Dataset: "site-a", Time: &server.Query_TimeRange{TimeRange: &server.TimeRange{Start: timestamppb.Now(), End: timestamppb.New(time.Now().Add(time.Hour))}}}, 0, false},
		{"Setting the end time before data's added returns nothing", &server.Query{Dataset: "site-a", Time: &server.Query_TimeRange{TimeRange: &server.TimeRange{End: timestamppb.New(start)}}}, 0, false},
		{"Selecting a single column only returns that column", &server.Query{Dataset: "site-a", X: &server.Query_XValue{XValue: 3}}, 10, false},
		{"Querying an X range returns a subset of data", &server.Query{Dataset: "site-a", X: &server.Query_XRange{XRange: &server.QueryRange{Start: 3, End: 5}}}, 20, false},
		{"Querying the whole X range returns all data", &server.Query{Dataset: "site-a", X: &server.Query_XAll{}}, 100, false},
		{"Selecting a single row only returns that row", &server.Query{Dataset: "site-a", Y: &server.Query_YValue{YValue: 3}}, 10, false},
		{"Querying a r range returns a subset of data", &server.Query{Dataset: "site-a", Y: &server.Query_YRange{YRange: &server.QueryRange{Start: 3, End: 5}}}, 20, false},
		{"Querying the whole Y range returns all data", &server.Query{Dataset: "site-a", Y: &server.Query_YAll{}}, 100, false},
		{"Selecting a single theta value with data returns all that data", &server.Query{Dataset: "site-a", T: &server.Query_TValue{TValue: 90}}, 100, false},
		{"Selecting a single theta value with no data returns nothing", &server.Query{Dataset: "site-a", T: &server.Query_TValue{TValue: 91}}, 0, false},
		{"Querying a theta range returns matching data", &server.Query{Dataset: "site-a", T: &server.Query_TRange{TRange: &server.QueryRange{Start: 0, End: 180}}}, 100, false},
		{"Querying the whole theta range returns all data", &server.Query{Dataset: "site-a", T: &server.Query_TAll{}}, 100, false},
		{"Querying a single location for all thetas returns a single value", &server.Query{Dataset: "site-a", X: &server.Query_XValue{XValue: 3}, Y: &server.Query_YValue{YValue: 3}}, 1, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			records, err := d.RetrieveRecords(test.query)
			if err == nil && test.expectError {
				t.Errorf("expected error, received none")
			} else if err != nil && !test.expectError {
				t.Errorf("unexpected error %#v", err)
			}

			rcvd := len(records)
			if test.expectCount != rcvd {
				t.Errorf("expected %d records, received %d", test.expectCount, rcvd)
			}
		})
	}
}

func BenchmarkDatabase_CreateDataset1(b *testing.B)    { benchmarkCreateDataset(1, b) }
func BenchmarkDatabase_CreateDataset2(b *testing.B)    { benchmarkCreateDataset(2, b) }
func BenchmarkDatabase_CreateDataset4(b *testing.B)    { benchmarkCreateDataset(4, b) }
func BenchmarkDatabase_CreateDataset8(b *testing.B)    { benchmarkCreateDataset(8, b) }
func BenchmarkDatabase_CreateDataset16(b *testing.B)   { benchmarkCreateDataset(16, b) }
func BenchmarkDatabase_CreateDataset32(b *testing.B)   { benchmarkCreateDataset(32, b) }
func BenchmarkDatabase_CreateDataset64(b *testing.B)   { benchmarkCreateDataset(64, b) }
func BenchmarkDatabase_CreateDataset128(b *testing.B)  { benchmarkCreateDataset(128, b) }
func BenchmarkDatabase_CreateDataset256(b *testing.B)  { benchmarkCreateDataset(256, b) }
func BenchmarkDatabase_CreateDataset512(b *testing.B)  { benchmarkCreateDataset(512, b) }
func BenchmarkDatabase_CreateDataset1024(b *testing.B) { benchmarkCreateDataset(1024, b) }

func benchmarkCreateDataset(i int32, b *testing.B) {
	d, err := New()
	if err != nil {
		b.Fatal(err)
	}

	names := make([]string, b.N)
	for idx := range names {
		names[idx] = randomStr()
	}

	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		d.CreateDataset(&server.Schema{
			Dataset:             names[j],
			XMin:                0,
			XMax:                i * 10,
			YMin:                0,
			YMax:                i * 10,
			LazyInitialAllocate: true,
			Frequency:           server.Frequency_F10000Hz,
		})

	}
}

func BenchmarkDatabase_InsertRecord1(b *testing.B)    { benchmarkInsertRecord(1, b) }
func BenchmarkDatabase_InsertRecord2(b *testing.B)    { benchmarkInsertRecord(2, b) }
func BenchmarkDatabase_InsertRecord4(b *testing.B)    { benchmarkInsertRecord(4, b) }
func BenchmarkDatabase_InsertRecord8(b *testing.B)    { benchmarkInsertRecord(8, b) }
func BenchmarkDatabase_InsertRecord16(b *testing.B)   { benchmarkInsertRecord(16, b) }
func BenchmarkDatabase_InsertRecord32(b *testing.B)   { benchmarkInsertRecord(32, b) }
func BenchmarkDatabase_InsertRecord64(b *testing.B)   { benchmarkInsertRecord(64, b) }
func BenchmarkDatabase_InsertRecord128(b *testing.B)  { benchmarkInsertRecord(128, b) }
func BenchmarkDatabase_InsertRecord256(b *testing.B)  { benchmarkInsertRecord(256, b) }
func BenchmarkDatabase_InsertRecord512(b *testing.B)  { benchmarkInsertRecord(512, b) }
func BenchmarkDatabase_InsertRecord1024(b *testing.B) { benchmarkInsertRecord(1024, b) }

func benchmarkInsertRecord(i int32, b *testing.B) {
	d, err := New()
	if err != nil {
		b.Fatal(err)
	}

	x := 10 * i
	y := 10 * i

	err = d.CreateDataset(&server.Schema{
		Dataset:   "site-a",
		XMin:      0,
		XMax:      x,
		YMin:      0,
		YMax:      y,
		Frequency: server.Frequency_F1Hz,
	})

	ts := timestamppb.Now()

	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		d.InsertRecord(&server.Record{
			Meta: &server.Metadata{
				When: ts,
			},
			Dataset: "site-a",
			Name:    "a-value",
			Value:   100,
			X:       x / 2,
			Y:       y / 2,
			T:       180,
		})

	}
}

func BenchmarkDatabase_InsertRecord_SortOnInsert1(b *testing.B) {
	benchmarkInsertRecord_SortOnInsert(1, b)
}
func BenchmarkDatabase_InsertRecord_SortOnInsert2(b *testing.B) {
	benchmarkInsertRecord_SortOnInsert(2, b)
}
func BenchmarkDatabase_InsertRecord_SortOnInsert4(b *testing.B) {
	benchmarkInsertRecord_SortOnInsert(4, b)
}
func BenchmarkDatabase_InsertRecord_SortOnInsert8(b *testing.B) {
	benchmarkInsertRecord_SortOnInsert(8, b)
}
func BenchmarkDatabase_InsertRecord_SortOnInsert16(b *testing.B) {
	benchmarkInsertRecord_SortOnInsert(16, b)
}
func BenchmarkDatabase_InsertRecord_SortOnInsert32(b *testing.B) {
	benchmarkInsertRecord_SortOnInsert(32, b)
}
func BenchmarkDatabase_InsertRecord_SortOnInsert64(b *testing.B) {
	benchmarkInsertRecord_SortOnInsert(64, b)
}
func BenchmarkDatabase_InsertRecord_SortOnInsert128(b *testing.B) {
	benchmarkInsertRecord_SortOnInsert(128, b)
}
func BenchmarkDatabase_InsertRecord_SortOnInsert256(b *testing.B) {
	b.Skip("Ends up getting OOM Killed on development machines; not enough memory for tens of millions of records")

	benchmarkInsertRecord_SortOnInsert(256, b)
}
func BenchmarkDatabase_InsertRecord_SortOnInsert512(b *testing.B) {
	b.Skip("Ends up getting OOM Killed on development machines; not enough memory for tens of millions of records")

	benchmarkInsertRecord_SortOnInsert(512, b)
}
func BenchmarkDatabase_InsertRecord_SortOnInsert1024(b *testing.B) {
	b.Skip("Ends up getting OOM Killed on development machines; not enough memory for tens of millions of records")

	benchmarkInsertRecord_SortOnInsert(1024, b)
}

func benchmarkInsertRecord_SortOnInsert(i int32, b *testing.B) {
	d, err := New()
	if err != nil {
		b.Fatal(err)
	}

	x := 10 * i
	y := 10 * i

	err = d.CreateDataset(&server.Schema{
		Dataset:      "site-a",
		XMin:         0,
		XMax:         x,
		YMin:         0,
		YMax:         y,
		Frequency:    server.Frequency_F1000Hz,
		SortOnInsert: true,
	})

	ts := timestamppb.Now()

	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		d.InsertRecord(&server.Record{
			Meta: &server.Metadata{
				When: ts,
			},
			Dataset: "site-a",
			Name:    "a-value",
			Value:   100,
			X:       x / 2,
			Y:       y / 2,
			T:       180,
		})

	}
}

func BenchmarkDatabase_Query1(b *testing.B)    { benchmarkQuery(1, b) }
func BenchmarkDatabase_Query2(b *testing.B)    { benchmarkQuery(2, b) }
func BenchmarkDatabase_Query4(b *testing.B)    { benchmarkQuery(4, b) }
func BenchmarkDatabase_Query8(b *testing.B)    { benchmarkQuery(8, b) }
func BenchmarkDatabase_Query16(b *testing.B)   { benchmarkQuery(16, b) }
func BenchmarkDatabase_Query32(b *testing.B)   { benchmarkQuery(32, b) }
func BenchmarkDatabase_Query64(b *testing.B)   { benchmarkQuery(64, b) }
func BenchmarkDatabase_Query128(b *testing.B)  { benchmarkQuery(128, b) }
func BenchmarkDatabase_Query256(b *testing.B)  { benchmarkQuery(256, b) }
func BenchmarkDatabase_Query512(b *testing.B)  { benchmarkQuery(512, b) }
func BenchmarkDatabase_Query1024(b *testing.B) { benchmarkQuery(1024, b) }

func benchmarkQuery(i int32, b *testing.B) {
	d, err := New()
	if err != nil {
		b.Fatal(err)
	}

	err = d.CreateDataset(&server.Schema{
		Dataset:             "site-a",
		XMin:                0,
		XMax:                i * 10,
		YMin:                0,
		YMax:                i * 10,
		Frequency:           server.Frequency_F1000Hz,
		LazyInitialAllocate: true,
	})

	ts := time.Now()
	start := timestamppb.New(ts.Add(-time.Minute))
	end := timestamppb.New(ts.Add(time.Minute))

	d.InsertRecord(&server.Record{
		Meta: &server.Metadata{
			When: timestamppb.New(ts),
		},
		Dataset: "site-a",
		Name:    "a-value",
		Value:   100,
		X:       5,
		Y:       5,
		T:       180,
	})

	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		d.RetrieveRecords(&server.Query{
			Dataset: "site-a",
			X:       new(server.Query_XAll),
			Y:       new(server.Query_YAll),
			T:       new(server.Query_TAll),
			Time: &server.Query_TimeRange{
				TimeRange: &server.TimeRange{
					Start: start,
					End:   end,
				},
			},
		})
	}
}

func BenchmarkDatabase_Query_All1(b *testing.B)    { benchmarkQuery(1, b) }
func BenchmarkDatabase_Query_All2(b *testing.B)    { benchmarkQuery(2, b) }
func BenchmarkDatabase_Query_All4(b *testing.B)    { benchmarkQuery(4, b) }
func BenchmarkDatabase_Query_All8(b *testing.B)    { benchmarkQuery(8, b) }
func BenchmarkDatabase_Query_All16(b *testing.B)   { benchmarkQuery(16, b) }
func BenchmarkDatabase_Query_All32(b *testing.B)   { benchmarkQuery(32, b) }
func BenchmarkDatabase_Query_All64(b *testing.B)   { benchmarkQuery(64, b) }
func BenchmarkDatabase_Query_All128(b *testing.B)  { benchmarkQuery(128, b) }
func BenchmarkDatabase_Query_All256(b *testing.B)  { benchmarkQuery(256, b) }
func BenchmarkDatabase_Query_All512(b *testing.B)  { benchmarkQuery(512, b) }
func BenchmarkDatabase_Query_All1024(b *testing.B) { benchmarkQuery(1024, b) }

func benchmarkQuery_All(i int32, b *testing.B) {
	d, err := New()
	if err != nil {
		b.Fatal(err)
	}

	err = d.CreateDataset(&server.Schema{
		Dataset:             "site-a",
		XMin:                0,
		XMax:                i * 10,
		YMin:                0,
		YMax:                i * 10,
		Frequency:           server.Frequency_F1000Hz,
		LazyInitialAllocate: true,
	})

	ts := time.Now()

	d.InsertRecord(&server.Record{
		Meta: &server.Metadata{
			When: timestamppb.New(ts),
		},
		Dataset: "site-a",
		Name:    "a-value",
		Value:   100,
		X:       5,
		Y:       5,
		T:       180,
	})

	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		d.RetrieveRecords(&server.Query{
			Dataset: "site-a",
			X:       new(server.Query_XAll),
			Y:       new(server.Query_YAll),
			T:       new(server.Query_TAll),
			Time:    &server.Query_TimeAll{},
		})
	}
}

func randomStr() (s string) {
	for i := 0; i < 16; i++ {
		s += string(chars[r.Int()%len(chars)])
	}

	return
}
