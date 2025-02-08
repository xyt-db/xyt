package xyt

import (
	"errors"
	"fmt"
)

type (
	position              int8
	coordRangeErrorReason int8
)

const (
	positionUnknown position = iota
	positionX
	positionY
	positionTheta
)

const (
	coordRangeErrorReasonDefault coordRangeErrorReason = iota
	coordRangeErrorReasonMinMax
)

func (p position) String() string {
	switch p {
	case positionX:
		return "X"

	case positionY:
		return "Y"

	case positionTheta:
		return "T"

	default:
		return ""
	}
}

func (c coordRangeErrorReason) String() string {
	switch c {
	case coordRangeErrorReasonMinMax:
		return "Min value must be less than Max value"

	default:
		return ""
	}
}

type PositionOutOfBoundsError struct {
	dataset  string
	position position
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
	position position
	reason   coordRangeErrorReason
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
	UnsortedDataset       = errors.New("Selecting the latest record on an un-sorted dataset makes no sense")
)
