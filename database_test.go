package xyt

import (
	"testing"
	"time"

	"github.com/xyt-db/xyt/server"
	"google.golang.org/protobuf/types/known/timestamppb"
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

		{"Happy path", &server.Record{Dataset: "site-a", X: 1, Y: 1, T: 90, Name: "temperature", Meta: &server.Metadata{When: timestamppb.New(time.Now())}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			d, err := New()
			if err != nil {
				// If this fails then there's no'ope
				t.Fatal(err)
			}

			err = d.CreateDataset(&server.Schema{
				Dataset: "site-a",
				XMin:    0,
				XMax:    10,
				YMin:    0,
				YMax:    10,
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
		Dataset: "site-a",
		XMin:    0,
		XMax:    10,
		YMin:    0,
		YMax:    10,
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
		{"Selecting a single column only returns that column", &server.Query{Dataset: "site-a", XMin: 3, XMax: 4}, 10, false},
		{"Empty query returns all data", &server.Query{Dataset: "site-a"}, 100, false},
		{"Setting end after time after the start returns nothing", &server.Query{Dataset: "site-a", From: timestamppb.Now()}, 0, false},
		{"Setting the end time before data's added returns nothing", &server.Query{Dataset: "site-a", Until: timestamppb.New(start)}, 0, false},
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
