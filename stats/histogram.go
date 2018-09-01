package stats

import (
	"fmt"
	"sync"
)

// thanks zserge/metric!

const maxBins = 100

type bin struct {
	value float64
	count float64
}

type Histogram struct {
	sync.Mutex
	bins  []bin
	total uint64
}

func NewHistogram() *Histogram {
	return &Histogram{
		Mutex: sync.Mutex{},
	}
}

func (h *Histogram) String() string {
	return fmt.Sprintf("min:\t%.2f\np50:\t%.2f\np90:\t%.2f\np95:\t%.2f\np99:\t%.2f\nmax:\t%.2f",
		h.Quantile(0.0),
		h.Quantile(0.5),
		h.Quantile(0.90),
		h.Quantile(0.95),
		h.Quantile(0.99),
		h.Quantile(1.0))
}

func (h *Histogram) Reset() {
	h.Lock()
	defer h.Unlock()
	h.bins = nil
	h.total = 0
}

func (h *Histogram) Add(n float64) {
	h.Lock()
	defer h.Unlock()
	defer h.trim()
	h.total++
	newbin := bin{value: n, count: 1}
	for i := range h.bins {
		if h.bins[i].value > n {
			h.bins = append(h.bins[:i], append([]bin{newbin}, h.bins[i:]...)...)
			return
		}
	}

	h.bins = append(h.bins, newbin)
}

func (h *Histogram) trim() {
	for len(h.bins) > maxBins {
		d := float64(0)
		i := 0
		for j := 1; j < len(h.bins); j++ {
			if dv := h.bins[j].value - h.bins[j-1].value; dv < d || j == 1 {
				d = dv
				i = j
			}
		}
		count := h.bins[i-1].count + h.bins[i].count
		merged := bin{
			value: (h.bins[i-1].value*h.bins[i-1].count + h.bins[i].value*h.bins[i].count) / count,
			count: count,
		}
		h.bins = append(h.bins[:i-1], h.bins[i:]...)
		h.bins[i-1] = merged
	}
}

func (h *Histogram) Quantile(q float64) float64 {
	count := q * float64(h.total)
	for i := range h.bins {
		count -= float64(h.bins[i].count)
		if count <= 0 {
			return h.bins[i].value
		}
	}
	return 0
}
