package pilosa

import (
	"fmt"
	"path/filepath"
	"plugin"
	"strings"

	"github.com/pilosa/pilosa/ext"
	"github.com/pilosa/pilosa/pql"
	"github.com/pilosa/pilosa/roaring"
)

func loadPlugin(path string, s *Server) error {
	p, err := plugin.Open(path)
	if err != nil {
		return err
	}
	s.logger.Printf("opening possible plugin %s\n", filepath.Base(path))
	pluginBitmapOps, err := p.Lookup("BitmapOps")
	if err != nil {
		s.logger.Printf("no BitmapOps found, ignoring plugin\n")
		return nil
	}
	opsFunc, ok := pluginBitmapOps.(func() ([]ext.BitmapOp, error))
	if !ok {
		return fmt.Errorf("BitmapOps exists, but type is '%T'", pluginBitmapOps)
	}
	bitmapOps, err := opsFunc()
	if err != nil {
		return err
	}
	bmOps, countOps := 0, 0
	for i := range bitmapOps {
		// title-case the name
		bitmapOps[i].Name = strings.Title(bitmapOps[i].Name)
		if bitmapOps[i].CountFunc != nil && bitmapOps[i].BitmapFunc != nil {
			return fmt.Errorf("op '%s' has both count and bitmap functions", bitmapOps[i].Name)
		}
		if bitmapOps[i].CountFunc == nil && bitmapOps[i].BitmapFunc == nil {
			return fmt.Errorf("op '%s' has neither count nor bitmap function", bitmapOps[i].Name)
		}
		if bitmapOps[i].CountFunc != nil {
			countOps++
		}
		if bitmapOps[i].BitmapFunc != nil {
			bmOps++
		}
	}
	s.logger.Printf("got %d bitmap ops, %d count ops\n", bmOps, countOps)
	s.executor.registerOps(bitmapOps)
	pql.RegisterPluginFuncs(bitmapOps)
	return nil
}

func WrapBitmap(bm *roaring.Bitmap) ext.Bitmap {
	return wrappedBitmap{bm}
}

type wrappedBitmap struct{ *roaring.Bitmap }

func UnwrapBitmap(bm ext.Bitmap) *roaring.Bitmap {
	return bm.(wrappedBitmap).Bitmap
}

func (b wrappedBitmap) Intersect(other ext.Bitmap) ext.Bitmap {
	return wrappedBitmap{b.Bitmap.Intersect(other.(wrappedBitmap).Bitmap)}
}
func (b wrappedBitmap) Union(other ext.Bitmap) ext.Bitmap {
	return wrappedBitmap{b.Bitmap.Union(other.(wrappedBitmap).Bitmap)}
}
func (b wrappedBitmap) IntersectionCount(other ext.Bitmap) uint64 {
	return b.Bitmap.IntersectionCount(other.(wrappedBitmap).Bitmap)
}
func (b wrappedBitmap) Difference(other ext.Bitmap) ext.Bitmap {
	return wrappedBitmap{b.Bitmap.Difference(other.(wrappedBitmap).Bitmap)}
}
func (b wrappedBitmap) Xor(other ext.Bitmap) ext.Bitmap {
	return wrappedBitmap{b.Bitmap.Xor(other.(wrappedBitmap).Bitmap)}
}
func (b wrappedBitmap) Shift(n int) (ext.Bitmap, error) {
	shifted, err := b.Bitmap.Shift(n)
	return wrappedBitmap{shifted}, err
}
func (b wrappedBitmap) Flip(start, last uint64) ext.Bitmap {
	return wrappedBitmap{b.Bitmap.Flip(start, last)}
}
