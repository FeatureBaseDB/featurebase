// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"log"
	"os"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/rbf"
)

func main() {

	if len(os.Args) != 3 {
		log.Fatal("USAGE convert srcPath destPath")

	}
	holder := pilosa.NewHolder(os.Args[1], nil)
	err := holder.Open()

	if err != nil {
		log.Fatal(err)

	}
	c := &pilosa.RBFConverter{
		Dbs:  make(map[string]*rbf.DB),
		Base: os.Args[2],
	}
	holder.ConvertToRBF(c)
}
