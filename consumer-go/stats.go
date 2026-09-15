package main

import (
	"fmt"
	"sort"
	"strings"
)

// Stats tracks how many messages were received per routing key.
type Stats struct {
	counts map[string]int64
}

func NewStats() *Stats {
	return &Stats{counts: make(map[string]int64)}
}

func (s *Stats) Record(routingKey string) {
	s.counts[routingKey]++
}

func (s *Stats) Total() int64 {
	var total int64
	for _, count := range s.counts {
		total += count
	}
	return total
}

func (s *Stats) CountFor(routingKey string) int64 {
	return s.counts[routingKey]
}

func (s *Stats) String() string {
	keys := make([]string, 0, len(s.counts))
	for key := range s.counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys)+1)
	parts = append(parts, fmt.Sprintf("total=%d", s.Total()))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, s.counts[key]))
	}
	return strings.Join(parts, " ")
}
