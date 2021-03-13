package fragment_vector

import (
	"math"
	"sync"
	"sync/atomic"
)

const (
	MaxFrags       = 100
	FragNumBits    = 56
	FragOffsetBits = 64 - FragNumBits
	FragSize       = int64(1) << FragOffsetBits
	FragNumMask    = math.MinInt64 >> (FragNumBits - 1)
	FragOffsetMask = ^FragNumMask
)

type FragmentVector struct {
	fragments      [][]interface{}
	commitFrags    [][]bool
	committedIndex int64
	ticketIndex    int64
	fragMu         sync.RWMutex
	commitMu       sync.Mutex
}

func NewFragmentVector() *FragmentVector {
	fv := &FragmentVector{
		fragments:      make([][]interface{}, MaxFrags, MaxFrags),
		commitFrags:    make([][]bool, MaxFrags, MaxFrags),
		committedIndex: -1,
		ticketIndex:    -1,
	}
	return fv
}

func (f *FragmentVector) GetIndex(index int64) interface{} {
	if index < 0 {
		return nil
	}
	ci := f.syncCommittedIndex()
	if ci < 0 || index > ci {
		return nil
	}
	return f.getIndexUnsafe(index)
}

func (f *FragmentVector) syncCommittedIndex() int64 {
	f.commitMu.Lock()
	defer f.commitMu.Unlock()
	newCi := f.committedIndex
	for i := f.committedIndex + 1; i < MaxFrags*FragSize; i++ {
		fn := i & FragNumMask >> FragOffsetBits
		fo := i & FragOffsetMask
		commitFrag := f.commitFrags[fn]
		if commitFrag == nil {
			break
		}
		commitFlag := commitFrag[fo]
		if !commitFlag {
			break
		}
		newCi = i
	}
	f.committedIndex = newCi
	return f.committedIndex
}

func (f *FragmentVector) getIndexUnsafe(index int64) interface{} {
	fn := index & FragNumMask >> FragOffsetBits
	fo := index & FragOffsetMask
	return f.fragments[fn][fo]
}

func (f *FragmentVector) Append(value interface{}) {
	ticket := atomic.AddInt64(&f.ticketIndex, 1)
	fn := ticket & FragNumMask >> FragOffsetBits
	fo := ticket & FragOffsetMask
	f.fragMu.RLock()
	fragment := &f.fragments[fn]
	commitFrag := &f.commitFrags[fn]
	if *fragment == nil {
		f.fragMu.RUnlock()
		f.fragMu.Lock()
		if *fragment == nil {
			*fragment = make([]interface{}, FragSize, FragSize)
			*commitFrag = make([]bool, FragSize, FragSize)
		}
		f.fragMu.Unlock()
	} else {
		f.fragMu.RUnlock()
	}
	(*fragment)[fo] = value
	(*commitFrag)[fo] = true
}

func (f *FragmentVector) Iterator() *fvIterator {
	return newFvIterator(f)
}

func (f *FragmentVector) View() *fvView {
	return newFvView(f)
}
