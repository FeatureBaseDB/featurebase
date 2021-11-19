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
	ageField()
	ipField()
	arbIdField()
	optInField()
	countryField()
	timeField()
}

func ageField() {
	csvFile, err := os.Create("age.csv")
	if err != nil {
		log.Fatalf("failed to create file %s", err)
	}

	writer := bufio.NewWriter(csvFile)

	for i := 0; i < totalRecords; i++ {
		if rand.Intn(100) <= 20 {
			_, err := writer.WriteString(strconv.Itoa(i) + "," + strconv.Itoa(rand.Intn(88)+13) + "\n")
			if err != nil {
				log.Fatalf("failed to write to age.csv %s", err)
			}

			if rand.Intn(10) == 1 {
				_, err := writer.WriteString(strconv.Itoa(i) + "," + strconv.Itoa(rand.Intn(88)+13) + "\n")
				if err != nil {
					log.Fatalf("failed to write to age.csv %s", err)
				}
			}

		}
	}
	writer.Flush()
	csvFile.Close()

}

func ipField() {
	csvFile, err := os.Create("ip.csv")
	if err != nil {
		log.Fatalf("failed to create file %s", err)
	}

	writer := bufio.NewWriter(csvFile)

	for i := 0; i < totalRecords; i++ {
		_, err := writer.WriteString(strconv.Itoa(i) + "," + fmt.Sprint(rand.Int31()) + "\n")
		if err != nil {
			log.Fatalf("failed to write to ip.csv %s", err)
		}

		if rand.Intn(10) == 1 {
			_, err := writer.WriteString(strconv.Itoa(i) + "," + fmt.Sprint(rand.Int31()) + "\n")
			if err != nil {
				log.Fatalf("failed to write to ip.csv %s", err)
			}
		}
	}

	writer.Flush()
	csvFile.Close()

}

func arbIdField() {
	csvFile, err := os.Create("identifier.csv")
	if err != nil {
		log.Fatalf("failed to create file %s", err)
	}

	writer := bufio.NewWriter(csvFile)

	for i := 0; i < totalRecords; i++ {
		_, err := writer.WriteString(strconv.Itoa(i) + "," + fmt.Sprint(rand.Int63()) + "\n")
		if err != nil {
			log.Fatalf("failed to write to identifier.csv %s", err)
		}

		if rand.Intn(10) == 1 {
			_, err := writer.WriteString(strconv.Itoa(i) + "," + fmt.Sprint(rand.Int63()) + "\n")
			if err != nil {
				log.Fatalf("failed to write to identifier.csv %s", err)
			}
		}
	}

	writer.Flush()
	csvFile.Close()

}

func timeField() {
	csvFile, err := os.Create("time.csv")
	if err != nil {
		log.Fatalf("failed to create file %s", err)
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
			log.Fatalf("failed to write to time.csv %s", err)
		}

		if rand.Intn(5) == 1 {
			date = generateDate()
			_, err := writer.WriteString(urlString + "," + strconv.Itoa(i) + "," + date + "\n")
			if err != nil {
				log.Fatalf("failed to write to time.csv %s", err)
			}
		}

		if rand.Intn(10) == 1 {
			value := distro.Uint64()
			urlString = "https://www.test.com/" + strconv.FormatUint(value, 10)
			date = generateDate()
			_, err := writer.WriteString(urlString + "," + strconv.Itoa(i) + "," + date + "\n")
			if err != nil {
				log.Fatalf("failed to write to time.csv %s", err)
			}
		}

	}

	writer.Flush()
	csvFile.Close()

}

func optInField() {
	csvFile, err := os.Create("optin.csv")
	if err != nil {
		log.Fatalf("failed to create file %s", err)
	}

	writer := bufio.NewWriter(csvFile)

	var optString string

	for i := 0; i < totalRecords; i++ {

		if rand.Intn(20) == 1 { //odds that optRandom == 1 are approx 5%
			optString = "1"
		} else {
			optString = "0"
		}

		_, err := writer.WriteString(optString + "," + strconv.Itoa(i) + "\n")
		if err != nil {
			log.Fatalf("failed to write to optin.csv %s", err)
		}

		if rand.Intn(10) == 1 {
			if optString == "0" {
				optString = "1"
				_, err := writer.WriteString(optString + "," + strconv.Itoa(i) + "\n")
				if err != nil {
					log.Fatalf("failed to write to optin.csv %s", err)
				}
			} else {
				optString = "0"
				_, err := writer.WriteString(optString + "," + strconv.Itoa(i) + "\n")
				if err != nil {
					log.Fatalf("failed to write to optin.csv %s", err)
				}
			}
		}
	}

	writer.Flush()
	csvFile.Close()

}

func countryField() {
	csvFile, err := os.Create("country.csv")
	if err != nil {
		log.Fatalf("failed to create file %s", err)
	}

	writer := bufio.NewWriter(csvFile)

	//populate countries
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
				log.Fatalf("failed to write to country.csv %s", err)
			}

			if rand.Intn(10) == 1 {
				value = distro.Uint64()
				country = countryList[value]
				_, err := writer.WriteString(country + "," + strconv.Itoa(i) + "\n")
				if err != nil {
					log.Fatalf("failed to write to country.csv %s", err)
				}
			}
		}

	}

	writer.Flush()
	csvFile.Close()

}

func generateDate() string {
	// generates a random date in format YYYY-MM-DDT00:00
	// does not include leap days or populate the hour/minute time
	year := strconv.Itoa(rand.Intn(20) + 2002) //year range 2002-2021
	month := rand.Intn(12) + 1
	day := rand.Intn(30) + 1

	if month == 2 && day >= 29 {
		day = 28
	}
	if month == 4 && day == 31 {
		day = 30
	}
	if month == 6 && day == 31 {
		day = 30
	}
	if month == 11 && day == 31 {
		day = 30
	}

	monthFix := fmt.Sprintf("%02d", month)
	dayFix := fmt.Sprintf("%02d", day)
	YMDstring := year + "-" + monthFix + "-" + dayFix + "T00:00"

	return YMDstring
}
