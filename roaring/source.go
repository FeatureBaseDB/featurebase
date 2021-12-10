// Copyright 2021 Molecula Corp. All rights reserved.
package roaring

import (
	"strings"
)

// A Source represents the source a given bitmap gets its data from,
// such as a memory-mapped file. When combining bitmaps, we might
// track them together in a single combined-source of some sort.
type Source interface {
	ID() string
	Dead() bool
}

// MergeSources combines sources. If you have two bitmaps, and you're
// combining them, then the combination's source is a combination of
// those two sources.
func MergeSources(sources ...Source) Source {
	sourceCount := 0
	totalCount := 0
	var lastSource Source
	for _, s := range sources {
		if s == nil {
			continue
		}
		lastSource = s
		if s, ok := s.(combinedSource); ok {
			sourceCount++
			totalCount += len(s)
		} else {
			sourceCount++
			totalCount++
		}
	}
	// if there's no sources (this includes all sources being
	// empty combinedSources), we don't have a source.
	if totalCount == 0 {
		return nil
	}
	// if there's exactly one source, combined or otherwise, that's
	// fine, we'll just return it.
	if sourceCount == 1 {
		return lastSource
	}
	// make a new combinedSource, flattening any combinedSources
	// already present.
	newSources := make([]Source, 0, totalCount)
	for _, s := range sources {
		if s == nil {
			continue
		}
		if s, ok := s.(combinedSource); ok {
			newSources = append(newSources, s...)
		} else {
			newSources = append(newSources, s)
		}
	}
	return combinedSource(newSources)
}

// SetSource tells the bitmap what source to associate with new things it
// creates. This is possibly logically incorrect.
func (b *Bitmap) SetSource(s Source) {
	b.Source = s
}

type combinedSource []Source

func (c combinedSource) ID() string {
	ids := make([]string, len(c))
	for i := range c {
		ids[i] = c[i].ID()
	}
	return strings.Join(ids, ",")
}

func (c combinedSource) Dead() bool {
	for i := range c {
		if c[i].Dead() {
			return true
		}
	}
	return false
}
