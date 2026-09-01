package model

import "testing"

func TestResourceFits(t *testing.T) {
	available := Resource{CPU: 2000, Memory: 4096, Disk: 20}
	if !available.Fits(Resource{CPU: 2000, Memory: 4096, Disk: 20}) {
		t.Fatal("equal resources should fit")
	}
	if available.Fits(Resource{CPU: 2001, Memory: 4096, Disk: 20}) {
		t.Fatal("larger CPU request should not fit")
	}
}
