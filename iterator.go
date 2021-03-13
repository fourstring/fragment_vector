package fragment_vector

type fvIterator struct {
	fv         *FragmentVector
	curIndex   int64
	limitIndex int64
}

func newFvIterator(fv *FragmentVector) *fvIterator {
	return &fvIterator{fv: fv,
		curIndex:   -1,
		limitIndex: fv.syncCommittedIndex(),
	}
}

func (i *fvIterator) Next() bool {
	i.curIndex++
	return i.curIndex <= i.limitIndex
}

func (i *fvIterator) Value() interface{} {
	return i.fv.getIndexUnsafe(i.curIndex)
}
