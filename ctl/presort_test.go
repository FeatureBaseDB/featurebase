package ctl_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/featurebasedb/featurebase/v3/logger"
)

func TestPreSort(t *testing.T) {
	td := t.TempDir()
	testFile(t, "sample.ndjson", td, sampleNDJSON)
	testFile(t, "sample.csv", td, sampleCSV)

	com := ctl.NewPreSortCommand(logger.StderrLogger)
	com.Type = "ndjson"
	com.OutputDir = "ndjson_out"
	com.File = "sample.ndjson"
	com.Table = "blah"
	com.PartitionN = 5
	com.JobSize = 2
	com.PrimaryKeyFields = []string{"url"}

	os.Chdir(td)

	err := com.Run(context.Background())
	if err != nil {
		t.Fatalf("running ndjson: %v", err)
	}

	entries, err := os.ReadDir(filepath.Join(td, "ndjson_out"))
	if err != nil {
		t.Fatalf("reading dir: %v", err)
	}
	totalSize := 0
	numWithData := 0
	totalSizeRead := 0
	if len(entries) != 5 {
		t.Errorf("expected 5 output files, but got: %d", len(entries))
	}
	for _, entry := range entries {
		stuff, err := os.ReadFile(filepath.Join(com.OutputDir, entry.Name()))
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		totalSizeRead += len(stuff)
		t.Logf("%s\n", stuff)
		if size(t, entry) > 0 {
			totalSize += int(size(t, entry))
			numWithData++
		}
		t.Log(entry.Name(), size(t, entry))
	}
	if totalSize < len(sampleNDJSON) || totalSize > len(sampleNDJSON) {
		t.Errorf("unexpected total data size orig: %d, got: %d", len(sampleNDJSON), totalSize)
	}

	com = ctl.NewPreSortCommand(logger.StderrLogger)
	com.Type = "csv"
	com.OutputDir = "csv_out"
	com.File = "sample.csv"
	com.Table = "blah"
	com.PartitionN = 5
	com.JobSize = 2
	com.PrimaryKeyFields = []string{"a", "b"}

	err = com.Run(context.Background())
	if err != nil {
		t.Fatalf("running csv: %v", err)
	}

	entries, err = os.ReadDir(filepath.Join(td, com.OutputDir))
	if err != nil {
		t.Fatalf("reading dir: %v", err)
	}
	totalSize = 0
	numWithData = 0
	totalSizeRead = 0
	if len(entries) != 5 {
		t.Errorf("expected 5 output files, but got: %d", len(entries))
	}
	for _, entry := range entries {
		stuff, err := os.ReadFile(filepath.Join(com.OutputDir, entry.Name()))
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		totalSizeRead += len(stuff)
		t.Logf("%s\n", stuff)
		if size(t, entry) > 0 {
			totalSize += int(size(t, entry))
			numWithData++
		}
		t.Log(entry.Name(), size(t, entry))
	}
	// -14 is removing the header
	if totalSize < len(sampleCSV)-14 || totalSize > len(sampleCSV)-14 {
		t.Errorf("unexpected total data size orig: %d, got: %d", len(sampleCSV), totalSize)
	}

}

func size(t *testing.T, e fs.DirEntry) int64 {
	info, err := e.Info()
	if err != nil {
		t.Fatalf(": %v", err)
	}
	return info.Size()
}

func testFile(t *testing.T, name, dir, contents string) {
	f, err := os.Create(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("creating temp file: %v", err)
	}
	_, err = f.WriteString(contents)
	if err != nil {
		t.Fatalf("writing temp file: %v", err)
	}
}

var sampleNDJSON string = `{"url":"https://www.yelp.com/search?find_desc=Desserts&find_loc=San+Jose,+CA&start=0","anything":"somedata","result":{"stuff":"blahhhhhhhhh","blufff":"hahahahah"}}
{"url":"https://www.yelp.com/search?find_desc=Desserts&find_loc=San+Jose,+CA&start=10","result":{"extractorData":{"url":"https://www.yelp.com/search?find_desc=Desserts&find_loc=San+Jose,+CA&start=10","data":[{"group":[{"Business":[{"href":"https://www.yelp.com/biz/oooh-san-jose-4?osq=Desserts","text":"Oooh"}]},{"Business":[{"href":"https://www.yelp.com/biz/hannah-san-jose?osq=Desserts","text":"Hannah"}]},{"Business":[{"href":"https://www.yelp.com/biz/chocatoo-san-jose?osq=Desserts","text":"Chocatoo"}]},{"Business":[{"href":"https://www.yelp.com/biz/nox-cookie-bar-san-jose?osq=Desserts","text":"Nox Cookie Bar"}]},{"Business":[{"href":"https://www.yelp.com/biz/sweet-fix-creamery-san-jose?osq=Desserts","text":"Sweet Fix Creamery"}]},{"Business":[{"href":"https://www.yelp.com/biz/my-milkshake-san-jose?osq=Desserts","text":"My Milkshake"}]},{"Business":[{"href":"https://www.yelp.com/biz/matcha-love-san-jose-6?osq=Desserts","text":"Matcha Love"}]},{"Business":[{"href":"https://www.yelp.com/biz/banana-cr%C3%AApe-san-jose-2?osq=Desserts","text":"Banana Crêpe"}]},{"Business":[{"href":"https://www.yelp.com/biz/marco-polo-italian-ice-cream-san-jose-4?osq=Desserts","text":"Marco Polo Italian Ice Cream"}]},{"Business":[{"href":"https://www.yelp.com/biz/blackball-desserts-san-jose-san-jose?osq=Desserts","text":"BlackBall Desserts San Jose"}]}]}]},"pageData":{"statusCode":200,"timestamp":1513286384917},"timestamp":1513286384917,"sequenceNumber":1}}
{"url":"https://www.yelp.com/search?find_desc=Desserts&find_loc=San+Jose,+CA&start=20","result":{"extractorData":{"url":"https://www.yelp.com/search?find_desc=Desserts&find_loc=San+Jose,+CA&start=20","data":[{"group":[{"Business":[{"href":"https://www.yelp.com/biz/anton-sv-p%C3%A2tisserie-san-jose-2?osq=Desserts","text":"Anton SV Pâtisserie"}]},{"Business":[{"href":"https://www.yelp.com/biz/soyful-desserts-san-jose-8?osq=Desserts","text":"Soyful Desserts"}]},{"Business":[{"href":"https://www.yelp.com/biz/cocola-bakery-san-jose?osq=Desserts","text":"Cocola Bakery"}]},{"Business":[{"href":"https://www.yelp.com/biz/charlies-cheesecake-works-san-jose?osq=Desserts","text":"Charlie’s Cheesecake Works"}]},{"Business":[{"href":"https://www.yelp.com/biz/jt-express-san-jose-2?osq=Desserts","text":"JT Express"}]},{"Business":[{"href":"https://www.yelp.com/biz/nox-cookie-bar-san-jose?osq=Desserts","text":"Nox Cookie Bar"}]},{"Business":[{"href":"https://www.yelp.com/biz/shuei-do-manju-shop-san-jose?osq=Desserts","text":"Shuei-Do Manju Shop"}]},{"Business":[{"href":"https://www.yelp.com/biz/churros-el-guero-san-jose?osq=Desserts","text":"Churros El Guero"}]},{"Business":[{"href":"https://www.yelp.com/biz/sno-crave-tea-house-san-jose-4?osq=Desserts","text":"Sno-Crave Tea House"}]},{"Business":[{"href":"https://www.yelp.com/biz/j-sweets-san-jose-3?osq=Desserts","text":"J.Sweets"}]}]}]},"pageData":{"statusCode":200,"timestamp":1513286395948},"timestamp":1513286395948,"sequenceNumber":2}}
{"url":"https://www.yelp.com/search?find_desc=Desserts&find_loc=San+Jose,+CA&start=30","result":{}}
{"url":"https://www.yelp.com/search?find_desc=Desserts&find_loc=San+Jose,+CA&start=40","result":{"extractorData":{"url":"https://www.yelp.com/search?find_desc=Desserts&find_loc=San+Jose,+CA&start=40","data":[{"group":[{"Business":[{"href":"https://www.yelp.com/biz/the-sweet-corner-san-jose?osq=Desserts","text":"The Sweet Corner"}]},{"Business":[{"href":"https://www.yelp.com/biz/marco-polo-italian-ice-cream-san-jose-4?osq=Desserts","text":"Marco Polo Italian Ice Cream"}]},{"Business":[{"href":"https://www.yelp.com/biz/honeyberry-san-jose-9?osq=Desserts","text":"Honeyberry"}]},{"Business":[{"href":"https://www.yelp.com/biz/my-ch%C3%A8-san-jose?osq=Desserts","text":"My Chè"}]},{"Business":[{"href":"https://www.yelp.com/biz/creme-paris-san-jose-3?osq=Desserts","text":"Cre’Me Paris"}]},{"Business":[{"href":"https://www.yelp.com/biz/snowflake-san-jose?osq=Desserts","text":"Snowflake"}]},{"Business":[{"href":"https://www.yelp.com/biz/willow-glen-creamery-san-jose-4?osq=Desserts","text":"Willow Glen Creamery"}]},{"Business":[{"href":"https://www.yelp.com/biz/vans-bakery-san-jose?osq=Desserts","text":"Van’s Bakery"}]},{"Business":[{"href":"https://www.yelp.com/biz/happiness-cafe-san-jose?osq=Desserts","text":"Happiness Cafe"}]},{"Business":[{"href":"https://www.yelp.com/biz/la-original-paleteria-y-neveria-san-jose?osq=Desserts","text":"La Original Paleteria Y Neveria"}]}]}]},"pageData":{"statusCode":200,"timestamp":1513286387763},"timestamp":1513286387763,"sequenceNumber":4}}
{"url":"https://www.yelp.com/search?find_desc=Desserts&find_loc=San+Jose,+CA&start=0","result":{}}
`

var sampleCSV string = `a,b,c,d,e,f,g
1,2,3,4,5,6,7
2,3,4,5,6,7,8
3,4,5,6,7,8,9
4,5,6,7,8,9,0
0,1,2,3,4,5,6
1,3,3,4,5,6,7
4,5,6,7,8,9,0
3,4,5,8,7,8,9
2,3,4,9,6,7,8
`
