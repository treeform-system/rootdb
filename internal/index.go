package internal

//one per table
type indexManager struct {
	primaryTree *tree
	columnName  string
}

func newIndexManager() *indexManager {
	var newIndices *indexManager = new(indexManager)

	return newIndices
}

func (i *indexManager) addIndex(dir, table, column string) error {
	var err error
	i.primaryTree, err = initializeTree(dir, table, column)
	if err != nil {
		return err
	}
	i.columnName = column
	return nil
}

func (i *indexManager) close() {
	i.primaryTree.closeIndex()
}
