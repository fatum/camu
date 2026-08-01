// Package metrics provides a small dependency-free Prometheus text registry.
// It is intentionally process-local: durable benchmark collection is handled
// by scraping the /metrics endpoint from each Camu node.
package metrics

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Registry struct {
	mu       sync.RWMutex
	families map[string]*family
}

type family struct {
	help    string
	typ     string
	samples map[string]sample
}

type sample struct {
	labels map[string]string
	value  float64
}

func NewRegistry() *Registry { return &Registry{families: make(map[string]*family)} }

func (r *Registry) Inc(name, help string, labels map[string]string) {
	r.Add(name, help, labels, 1)
}

func (r *Registry) Add(name, help string, labels map[string]string, value float64) {
	if value == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	f := r.ensureFamily(name, help, "counter")
	key := labelKey(labels)
	s := f.samples[key]
	s.labels = cloneLabels(labels)
	s.value += value
	f.samples[key] = s
}

func (r *Registry) Set(name, help string, labels map[string]string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f := r.ensureFamily(name, help, "gauge")
	key := labelKey(labels)
	f.samples[key] = sample{labels: cloneLabels(labels), value: value}
}

func (r *Registry) Observe(name, help string, labels map[string]string, duration time.Duration) {
	seconds := duration.Seconds()
	r.Add(name+"_seconds_sum", help, labels, seconds)
	r.Inc(name+"_seconds_count", help, labels)
}

func (r *Registry) Handler() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.families))
	for name := range r.families {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		f := r.families[name]
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, f.help, name, f.typ)
		keys := make([]string, 0, len(f.samples))
		for key := range f.samples {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			s := f.samples[key]
			fmt.Fprintf(&b, "%s%s %s\n", name, renderLabels(s.labels), strconv.FormatFloat(s.value, 'f', -1, 64))
		}
	}
	return b.String()
}

func (r *Registry) ensureFamily(name, help, typ string) *family {
	if f := r.families[name]; f != nil {
		return f
	}
	f := &family{help: help, typ: typ, samples: make(map[string]sample)}
	r.families[name] = f
	return f
}

func labelKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, key := range keys {
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteString(labels[key])
		b.WriteByte('\x00')
	}
	return b.String()
}

func cloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	copy := make(map[string]string, len(labels))
	for key, value := range labels {
		copy[key] = value
	}
	return copy
}

func renderLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=\"%s\"", key, escape(labels[key])))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func escape(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return strings.ReplaceAll(value, `"`, `\"`)
}
