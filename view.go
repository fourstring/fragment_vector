package fragment_vector

type fvView struct {
	fv         *FragmentVector
	limitIndex int64
	length     int64
}

func newFvView(fv *FragmentVector) *fvView {
	l := fv.syncCommittedIndex()
	return &fvView{fv: fv,
		limitIndex: l,
		length:     l + 1,
	}
}

func (v *fvView) GetIndex(index int64) interface{} {
	if index < 0 || index > v.limitIndex {
		return nil
	}
	return v.fv.getIndexUnsafe(index)
}

func (v *fvView) Len() int64 {
	return v.length
}
