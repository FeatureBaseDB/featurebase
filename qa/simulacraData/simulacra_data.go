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

// PURPOSE: Generate data for Samsung unique workflow/use case, as described in Jira Ticket FB-971
// INPUT: none
// OUTPUT: 6 csv files, containing approx 1 billion lines of data associated to 200 million unique records (approx 28BGB of data)

package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

var countryList [246]string = [...]string{"ABW", "AFG", "AGO", "AIA", "ALA", "ALB", "AND", "ANT", "ARE", "ARG",
	"ARM", "ASM", "ATA", "ATF", "ATG", "AUS", "AUT", "AZE", "BDI", "BEL", "BEN", "BFA", "BGD", "BGR", "BHR", "BHS",
	"BIH", "BLM", "BLR", "BLZ", "BMU", "BOL", "BRA", "BRB", "BRN", "BTN", "BVT", "BWA", "CAF", "CAN", "CCK", "CHE",
	"CHL", "CHN", "CIV", "CMR", "COD", "COG", "COK", "COL", "COM", "CPV", "CRI", "CUB", "CXR", "CYM", "CYP", "CZE",
	"DEU", "DJI", "DMA", "DNK", "DOM", "DZA", "ECU", "EGY", "ERI", "ESH", "ESP", "EST", "ETH", "FIN", "FJI", "FLK",
	"FRA", "FRO", "FSM", "GAB", "GBR", "GEO", "GGY", "GHA", "GIB", "GIN", "GLP", "GMB", "GNB", "GNQ", "GRC", "GRD",
	"GRL", "GTM", "GUF", "GUM", "GUY", "HKG", "HMD", "HND", "HRV", "HTI", "HUN", "IDN", "IMN", "IND", "IOT", "IRL",
	"IRN", "IRQ", "ISL", "ISR", "ITA", "JAM", "JEY", "JOR", "JPN", "KAZ", "KEN", "KGZ", "KHM", "KIR", "KNA", "KOR",
	"KWT", "LAO", "LBN", "LBR", "LBY", "LCA", "LIE", "LKA", "LSO", "LTU", "LUX", "LVA", "MAC", "MAF", "MAR", "MCO",
	"MDA", "MDG", "MDV", "MEX", "MHL", "MKD", "MLI", "MLT", "MMR", "MNE", "MNG", "MNP", "MOZ", "MRT", "MSR", "MTQ",
	"MUS", "MWI", "MYS", "MYT", "NAM", "NCL", "NER", "NFK", "NGA", "NIC", "NIU", "NLD", "NOR", "NPL", "NRU", "NZL",
	"OMN", "PAK", "PAN", "PCN", "PER", "PHL", "PLW", "PNG", "POL", "PRI", "PRK", "PRT", "PRY", "PSE", "PYF", "QAT",
	"REU", "ROU", "RUS", "RWA", "SAU", "SDN", "SEN", "SGP", "SGS", "SHN", "SJM", "SLB", "SLE", "SLV", "SMR", "SOM",
	"SPM", "SRB", "STP", "SUR", "SVK", "SVN", "SWE", "SWZ", "SYC", "SYR", "TCA", "TCD", "TGO", "THA", "TJK", "TKL",
	"TKM", "TLS", "TON", "TTO", "TUN", "TUR", "TUV", "TWN", "TZA", "UGA", "UKR", "UMI", "URY", "USA", "UZB", "VAT",
	"VCT", "VEN", "VGB", "VIR", "VNM", "VUT", "WLF", "WSM", "YEM", "ZAF", "ZMB", "ZWE",
}

const totalRecords int = 200000000

func main() {
	err0 := ageField()
	if err0 == nil {
		log.Fatalf("unable to generate age field: %v", err0)
	}

	err1 := ipField()
	if err1 != nil {
		log.Fatalf("unable to generate IPfield: %v", err1)
	}

	err2 := arbIdField()
	if err2 != nil {
		log.Fatalf("unable to generate indentifier field: %v", err2)
	}

	err3 := optInField()
	if err3 != nil {
		log.Fatalf("unable to generate opt in field: %v", err3)
	}
	err4 := countryField()
	if err4 != nil {
		log.Fatalf("unable to generate country field: %v", err4)
	}

	err5 := timeField()
	if err5 != nil {
		log.Fatalf("unable to generate time field: %v", err5)
	}
}

func ageField() error {
	csvFile, err0 := os.Create("age.csv")
	if err0 != nil {
		return errors.Wrap(err0, "unable to create age.csv")
	}

	writer := bufio.NewWriter(csvFile)

	for i := 0; i < totalRecords; i++ {
		if rand.Intn(100) <= 20 {
			_, err := writer.WriteString(strconv.Itoa(i) + "," + strconv.Itoa(rand.Intn(88)+13) + "\n")
			if err != nil {
				return errors.Wrap(err, "unable to write to age.csv")
			}

			if rand.Intn(10) == 1 {
				_, err := writer.WriteString(strconv.Itoa(i) + "," + strconv.Itoa(rand.Intn(88)+13) + "\n")
				if err != nil {
					return errors.Wrap(err, "unable to write to age.csv")

				}
			}

		}
	}
	err1 := writer.Flush()
	if err1 != nil {
		return errors.Wrap(err1, "unable to flush writer")
	}

	err2 := csvFile.Close()
	if err2 != nil {
		return errors.Wrap(err2, "unable to close age.csv")
	}
	return nil
}

func ipField() error {
	csvFile, err0 := os.Create("ip.csv")
	if err0 != nil {
		return errors.Wrap(err0, "unable to create ip.csv")
	}

	writer := bufio.NewWriter(csvFile)

	for i := 0; i < totalRecords; i++ {
		_, err := writer.WriteString(strconv.Itoa(i) + "," + fmt.Sprint(rand.Int31()) + "\n")
		if err != nil {
			return errors.Wrap(err, "unable to write to ip.csv")
		}

		if rand.Intn(10) == 1 {
			_, err := writer.WriteString(strconv.Itoa(i) + "," + fmt.Sprint(rand.Int31()) + "\n")
			if err != nil {
				return errors.Wrap(err, "unable to write to ip.csv")
			}
		}
	}

	err1 := writer.Flush()
	if err1 != nil {
		return errors.Wrap(err1, "unable to flush writer")
	}

	err2 := csvFile.Close()
	if err2 != nil {
		return errors.Wrap(err2, "unable to close ip.csv")
	}
	return nil

}

func arbIdField() error {
	csvFile, err0 := os.Create("identifier.csv")
	if err0 != nil {
		return errors.Wrap(err0, "unable to create identifier.csv")
	}

	writer := bufio.NewWriter(csvFile)

	for i := 0; i < totalRecords; i++ {
		_, err := writer.WriteString(strconv.Itoa(i) + "," + fmt.Sprint(rand.Int63()) + "\n")
		if err != nil {
			return errors.Wrap(err, "unable to write to identifier.csv")
		}

		if rand.Intn(10) == 1 {
			_, err := writer.WriteString(strconv.Itoa(i) + "," + fmt.Sprint(rand.Int63()) + "\n")
			if err != nil {
				return errors.Wrap(err, "unable to write to identifier.csv")
			}
		}
	}

	err1 := writer.Flush()
	if err1 != nil {
		return errors.Wrap(err1, "unable to flush writer")
	}

	err2 := csvFile.Close()
	if err2 != nil {
		return errors.Wrap(err2, "unable to close identifier.csv")
	}
	return nil

}

func timeField() error {
	csvFile, err0 := os.Create("time.csv")
	if err0 != nil {
		return errors.Wrap(err0, "unable to create time.csv")
	}

	writer := bufio.NewWriter(csvFile)

	r := rand.New(rand.NewSource(rand.Int63()))
	s := 1.01
	v := 1.01
	var imax uint64 = 4000000

	distro := rand.NewZipf(r, s, v, imax)

	for i := 0; i < totalRecords; i++ {
		value := distro.Uint64()
		urlString := "https://www.test.com/" + strconv.FormatUint(value, 10)

		date := generateDate()

		_, err := writer.WriteString(urlString + "," + strconv.Itoa(i) + "," + date + "\n")
		if err != nil {
			return errors.Wrap(err, "unable to write to time.csv")
		}

		if rand.Intn(5) == 1 {
			date = generateDate()
			_, err := writer.WriteString(urlString + "," + strconv.Itoa(i) + "," + date + "\n")
			if err != nil {
				return errors.Wrap(err, "unable to write to time.csv")
			}
		}

		if rand.Intn(10) == 1 {
			value := distro.Uint64()
			urlString = "https://www.test.com/" + strconv.FormatUint(value, 10)
			date = generateDate()
			_, err := writer.WriteString(urlString + "," + strconv.Itoa(i) + "," + date + "\n")
			if err != nil {
				return errors.Wrap(err, "unable to write to time.csv")
			}
		}

	}
	err1 := writer.Flush()
	if err1 != nil {
		return errors.Wrap(err1, "unable to flush writer")
	}

	err2 := csvFile.Close()
	if err2 != nil {
		return errors.Wrap(err2, "unable to close time.csv")
	}
	return nil

}

func optInField() error {
	csvFile, err0 := os.Create("optin.csv")
	if err0 != nil {
		return errors.Wrap(err0, "unable to create optin.csv")
	}

	writer := bufio.NewWriter(csvFile)

	var optString string

	for i := 0; i < totalRecords; i++ {

		if rand.Intn(20) == 1 {
			optString = "1"
		} else {
			optString = "0"
		}

		_, err := writer.WriteString(optString + "," + strconv.Itoa(i) + "\n")
		if err != nil {
			return errors.Wrap(err, "unable to write to optin.csv")
		}

		if rand.Intn(10) == 1 {
			if optString == "0" {
				optString = "1"
				_, err := writer.WriteString(optString + "," + strconv.Itoa(i) + "\n")
				if err != nil {
					return errors.Wrap(err, "unable to write to optin.csv")
				}
			} else {
				optString = "0"
				_, err := writer.WriteString(optString + "," + strconv.Itoa(i) + "\n")
				if err != nil {
					return errors.Wrap(err, "unable to write to optin.csv")
				}
			}
		}
	}

	err1 := writer.Flush()
	if err1 != nil {
		return errors.Wrap(err1, "unable to flush writer")
	}

	err2 := csvFile.Close()
	if err2 != nil {
		return errors.Wrap(err2, "unable to close optin.csv")
	}
	return nil

}

func countryField() error {
	csvFile, err0 := os.Create("country.csv")
	if err0 != nil {
		return errors.Wrap(err0, "unable to create country.csv")
	}

	writer := bufio.NewWriter(csvFile)

	// populate countries
	r := rand.New(rand.NewSource(rand.Int63()))
	s := 1.01
	v := 1.01
	var imax uint64 = 245

	distro := rand.NewZipf(r, s, v, imax)

	for i := 0; i < totalRecords; i++ {
		if rand.Intn(2) == 1 {
			value := distro.Uint64()
			country := countryList[value]
			_, err := writer.WriteString(country + "," + strconv.Itoa(i) + "\n")
			if err != nil {
				return errors.Wrap(err, "unable to write to country.csv")
			}

			if rand.Intn(10) == 1 {
				value = distro.Uint64()
				country = countryList[value]
				_, err := writer.WriteString(country + "," + strconv.Itoa(i) + "\n")
				if err != nil {
					return errors.Wrap(err, "unable to write to country.csv")
				}
			}
		}

	}

	err1 := writer.Flush()
	if err1 != nil {
		return errors.Wrap(err1, "unable to flush writer")
	}

	err2 := csvFile.Close()
	if err2 != nil {
		return errors.Wrap(err2, "unable to close country.csv")
	}
	return nil

}

func generateDate() string {
	// Nov 22, 2021 = 1637616960
	// twenty years =  631138520
	// date min 	= 1006478440
	date := rand.Int63n(631138520) + 1006478440

	t := time.Unix(date, 0)
	YMDstring := t.Format("2006-01-02T15:04")

	return YMDstring
}
