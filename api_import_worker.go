package pilosa

import (
	"encoding/binary"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/roaring"
	"github.com/pkg/errors"
)

func importWorker(importWork chan importJob) {
	for job := range importWork {
		err := importWorkerFunc(job)

		select {
		case <-job.ctx.Done():
		case job.errChan <- err:
		}
	}
}

func importWorkerFunc(job importJob) error {
	for viewName, viewData := range job.req.Views {
		// The logic here corresponds to the logic in fragment.cleanViewName().
		// Unfortunately, the logic in that method is not completely exclusive
		// (i.e. an "other" view named with format YYYYMMDD would be handled
		// incorrectly). One way to address this would be to change the logic
		// overall so there weren't conflicts. For now, we just
		// rely on the field type to inform the intended view name.
		if viewName == "" {
			viewName = viewStandard
		} else if job.field.Type() == FieldTypeTime {
			viewName = fmt.Sprintf("%s_%s", viewStandard, viewName)
		}
		if len(viewData) == 0 {
			return fmt.Errorf("no data to import for view: %s", viewName)
		}

		// TODO: deprecate ImportRoaringRequest.Clear, but
		// until we do, we need to check its value to provide
		// backward compatibility.
		doAction := job.req.Action
		if doAction == "" {
			if job.req.Clear {
				doAction = RequestActionClear
			} else {
				doAction = RequestActionSet
			}
		}

		if err := importWorkerTx(job, doAction, viewName, viewData); err != nil {
			return err
		}
	}
	return nil
}

func importWorkerTx(job importJob, doAction string, viewName string, viewData []byte) (txErr error) {
	tx, finisher, err := job.qcx.GetTx(Txo{Write: writable, Index: job.field.idx, Shard: job.shard})
	if err != nil {
		return err
	}
	defer finisher(&txErr)

	var doClear bool
	switch doAction {
	case RequestActionOverwrite:
		err := job.field.importRoaringOverwrite(job.ctx, tx, viewData, job.shard, viewName, job.req.Block)
		if err != nil {
			return errors.Wrap(err, "importing roaring as overwrite")
		}
	case RequestActionClear:
		doClear = true
		fallthrough
	case RequestActionSet:
		fileMagic := uint32(binary.LittleEndian.Uint16(viewData[0:2]))
		data := viewData
		if fileMagic != roaring.MagicNumber {
			// if the view data arrives is in the "standard" roaring format, we must
			// make a copy of data in order allow for the conversion to the pilosa roaring run format
			// in field.importRoaring
			data = make([]byte, len(viewData))
			copy(data, viewData)
		}
		if job.req.UpdateExistence {
			if ef := job.field.idx.existenceField(); ef != nil {
				existence, err := combineForExistence(data)
				if err != nil {
					return errors.Wrap(err, "merging existence on roaring import")
				}

				err = ef.importRoaring(job.ctx, tx, existence, job.shard, "standard", false)
				if err != nil {
					return errors.Wrap(err, "updating existence on roaring import")
				}
			}
		}

		err := job.field.importRoaring(job.ctx, tx, data, job.shard, viewName, doClear)

		if err != nil {
			return errors.Wrap(err, "importing standard roaring")
		}
	}
	return nil

}
