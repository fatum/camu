package server

import (
	"time"
)

func (s *Server) metricInc(name, help string, labels map[string]string) {
	if s.metrics != nil {
		s.metrics.Inc(name, help, labels)
	}
}

func (s *Server) metricAdd(name, help string, labels map[string]string, value float64) {
	if s.metrics != nil {
		s.metrics.Add(name, help, labels, value)
	}
}

func (s *Server) metricSet(name, help string, labels map[string]string, value float64) {
	if s.metrics != nil {
		s.metrics.Set(name, help, labels, value)
	}
}

func (s *Server) metricObserve(name, help string, labels map[string]string, duration time.Duration) {
	if s.metrics != nil {
		s.metrics.Observe(name, help, labels, duration)
	}
}
