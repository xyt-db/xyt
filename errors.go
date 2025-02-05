package xyt

import (
	"errors"
	"fmt"
)

type PositionOutOfBoundsError struct {
	dataset  string
	position string
	min, max int32
	received int32
}

func (e PositionOutOfBoundsError) Error() string {
	return fmt.Sprintf("invalid %s position for %s: expected between %d and %d, yet received %d",
		e.position, e.dataset, e.min, e.max, e.received,
	)
}

type InvalidCoordRangeError struct {
	dataset  string
	position string
	reason   string
}

func (e InvalidCoordRangeError) Error() string {
	return fmt.Sprintf("invalid %s for %s: %s",
		e.position, e.dataset, e.reason,
	)
}

var (
	DuplicateDatasetError = errors.New("Dataset already exists")
	EmptyRecordError      = errors.New("Record is empty, or otherwise nil")
	MissingDatasetError   = errors.New("Missing Dataset")
	MissingWhenError      = errors.New("Missing When value")
	MissingFieldNameError = errors.New("Missing Field Name value")
	UnknownDatasetError   = errors.New("Unknown Dataset")
	EmptySchemaError      = errors.New("Schema is empty, or otherwise nil")
)
