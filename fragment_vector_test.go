package fragment_vector

import (
	"context"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestFVFunctions(t *testing.T) {
	fv := NewFragmentVector()
	assert.Nil(t, fv.GetIndex(-1))

	for i := int64(0); i < 100*FragSize; i++ {
		fv.Append(i)
	}

	assert.Nil(t, fv.GetIndex(100*FragSize+1))

	j := int64(0)
	for i := fv.Iterator(); i.Next(); {
		assert.Equal(t, j, i.Value())
		j++
	}

	v := fv.View()
	for i := int64(0); i < v.Len(); i++ {
		assert.Equal(t, i, v.GetIndex(i))
	}
}

func TestGeneralSlice(t *testing.T) {
	v := make([]interface{}, 0)
	for i := int64(0); i < 100*FragSize; i++ {
		v = append(v, i)
	}

	for i := int64(0); i < 100*FragSize; i++ {
		assert.Equal(t, i, v[i])
	}
}

func TestFVPerformance(t *testing.T) {
	for i := 0; i < 100; i++ {
		fv := NewFragmentVector()
		wg := new(sync.WaitGroup)
		writer := func(fragNum int64) {
			base := fragNum * FragSize
			for i := base; i < base+FragSize; i++ {
				fv.Append(i)
			}
			wg.Done()
		}
		readerWg := new(sync.WaitGroup)
		reader := func(ctx context.Context) int64 {
			ret := int64(0)
			for true {
				select {
				case <-ctx.Done():
					readerWg.Done()
					return ret
				default:
					v := fv.View()
					for i := int64(0); i < v.Len(); i++ {
						ret = v.GetIndex(i).(int64)
					}
				}
			}
			return ret
		}
		readerCtx, cancel := context.WithCancel(context.Background())
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go writer(int64(i))
		}
		for i := 0; i < 10; i++ {
			readerWg.Add(1)
			go reader(readerCtx)
		}
		wg.Wait()
		cancel()
		readerWg.Wait()
	}
}

func TestSlicePerformance(t *testing.T) {
	for i := 0; i < 100; i++ {
		v := make([]interface{}, 0)
		mu := new(sync.Mutex)
		wg := new(sync.WaitGroup)
		writer := func(fragNum int64) {
			base := fragNum * FragSize
			for i := base; i < base+FragSize; i++ {
				mu.Lock()
				v = append(v, i)
				mu.Unlock()
			}
			wg.Done()
		}
		readerWg := new(sync.WaitGroup)
		reader := func(ctx context.Context) int64 {
			ret := int64(0)
			for true {
				select {
				case <-ctx.Done():
					readerWg.Done()
					return ret
				default:
					mu.Lock()
					for i := 0; i < len(v); i++ {
						ret = v[i].(int64)
					}
					mu.Unlock()
				}
			}
			return ret
		}
		readerCtx, cancel := context.WithCancel(context.Background())
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go writer(int64(i))
		}
		for i := 0; i < 10; i++ {
			readerWg.Add(1)
			go reader(readerCtx)
		}
		wg.Wait()
		cancel()
		readerWg.Wait()
	}
}
