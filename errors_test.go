package xyt

import (
	"testing"
)

func TestPositionOutOfBoundsError_Error(t *testing.T) {
	for _, test := range []struct {
		p      position
		expect string
	}{
		{positionX, "invalid X position for my-test-dataset: expected between 0 and 10, yet received 25"},
		{positionY, "invalid Y position for my-test-dataset: expected between 0 and 10, yet received 25"},
		{positionTheta, "invalid T position for my-test-dataset: expected between 0 and 10, yet received 25"},
		{positionUnknown, "invalid  position for my-test-dataset: expected between 0 and 10, yet received 25"},
	} {
		t.Run(test.p.String(), func(t *testing.T) {
			e := PositionOutOfBoundsError{
				dataset:  "my-test-dataset",
				position: test.p,
				min:      0,
				max:      10,
				received: 25,
			}

			rcvd := e.Error()

			if test.expect != rcvd {
				t.Errorf("expected %q, received %q", test.expect, rcvd)
			}
		})
	}
}

func TestInvalidCoordRangeError_Error(t *testing.T) {
	for _, test := range []struct {
		p      position
		r      coordRangeErrorReason
		expect string
	}{
		{positionX, coordRangeErrorReasonMinMax, "invalid X for my-test-dataset: Min value must be less than Max value"},
		{positionY, coordRangeErrorReasonMinMax, "invalid Y for my-test-dataset: Min value must be less than Max value"},
		{positionUnknown, coordRangeErrorReasonDefault, "invalid  for my-test-dataset: "},
	} {
		t.Run(test.p.String(), func(t *testing.T) {
			e := InvalidCoordRangeError{
				dataset:  "my-test-dataset",
				position: test.p,
				reason:   test.r,
			}

			rcvd := e.Error()

			if test.expect != rcvd {
				t.Errorf("expected %q, received %q", test.expect, rcvd)
			}
		})
	}
}
