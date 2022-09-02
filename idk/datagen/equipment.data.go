package datagen

var costs = map[string]map[string]minMax{
	"Power": {
		"Breaker":     {min: 10, max: 400},
		"Generator":   {min: 1000, max: 850000},
		"Air Handler": {min: 1000, max: 35000},
		"Chiller":     {min: 1000, max: 500000},
		"UPS":         {min: 200, max: 100000},
	},
	"Communications": {
		"Router": {min: 100, max: 210000},
		"Server": {min: 1000, max: 25000},
		"BTS":    {min: 500, max: 20000},
		"CPE":    {min: 40, max: 500},
	},
	"Transactions": {
		"POS": {min: 50, max: 1000},
	},
}

// domain,type,manufacturer,model
var domains = map[string]map[string]map[string][]string{
	"Power": {
		"Breaker": {
			"GE":              {"A Series", "FB100", "FC100", "J600", "Q-Line", "Spectra", "TEY"},
			"Westinghouse":    {"MCP331000", "MCP13300R", "KD3225", "JDC3200", "JDB3150", "HMCP150T4"},
			"Cutler-Hammer":   {"C Series", "EG Series", "G Series", "JG Series", "LG Series", "NZM1", "NZM2"},
			"Siemens":         {"SIRIUS 3RV29", "SIRIUS 3RV28", "SIRIUS 3RV27", "SIRIUS 3RV2773", "SIRIUS 3RT2024", "SIRIUS 3RV1611", "SIRIUS 3RV1714"},
			"ABB":             {"Tmax T", "Tmax XT", "ISOMAX"},
			"Federal Pacific": {"NA260", "NK-631000", "NEF431100", "NBGF20", "NFJ631225", "HEF631100"},
			"Square D":        {"QOB120", "QO120", "QO230", "QO220", "QO1515", "QO2020", "QO115PDF"},
			"PACS Industries": {"Type VAH", "Type VHA", "Type VHAL", "Model PHVX", "Model PHAX", "Model PHX,", "Model HAX", "15VAH5000", "VAH 13.8-63-50-27", "VAH 17-50-50-27", "FBO3121525-FS"},
		},
		"Generator": {
			"ABB":                      {"3AFP9105246", "3AFP9105265"},
			"Action":                   {"Gen-Act-0", "Gen-Act-1", "Gen-Act-2", "Gen-Act-3"}, // fake
			"Aggreko":                  {"YAAF003", "GHPII8065E", "GHPDC950", "GHPII8065E"},
			"Atlas Copco":              {"QIS 35", "QIS 45", "QIS 65", "QIS 95", "QIS 415", "QIS 505"},
			"Caterpillar":              {"C1.1", "C1.5", "C2.2", "C3.3", "C4.4"},
			"Cummins":                  {"D1703", "V2203", "4BT3.3", "S3.8"},
			"Himoinso":                 {"HYW-8 T5", "HYW-13 T5", "HYW-17 T5", "HYW-45 T5"},
			"Ingersoll Rand":           {"Gen-Ing-0", "Gen-Ing-1", "Gen-Ing-2", "Gen-Ing-3"}, // fake
			"Kirloskar":                {"KG1-125WS", "KG1-15AS", "KG1-40AS", "KG1-100WS"},
			"Manlift Group":            {"MPG-600", "MPG-650", "MPG-800", "MPG-1000", "MPG-1500", "MPG-2000"},
			"Generac":                  {"RG022", "RG04845", "RG027"},
			"Briggs & Stratton":        {"076191", "076195"},
			"Winco":                    {"Gen-Win-0", "Gen-Win-1", "Gen-Win-2", "Gen-Win-3", "Gen-Win-4", "Gen-Win-5"},              // fake
			"Westinghouse":             {"Gen-Wes-0", "Gen-Wes-1", "Gen-Wes-2", "Gen-Wes-3", "Gen-Wes-4"},                           // fake
			"DuroMax":                  {"Gen-Dur-0", "Gen-Dur-1", "Gen-Dur-2", "Gen-Dur-3", "Gen-Dur-4", "Gen-Dur-5", "Gen-Dur-6"}, // fake
			"DuroStar":                 {"Gen-Dur-0", "Gen-Dur-1", "Gen-Dur-2", "Gen-Dur-3"},                                        // fake
			"ATIMA":                    {"Gen-ATI-0", "Gen-ATI-1", "Gen-ATI-2", "Gen-ATI-3", "Gen-ATI-4", "Gen-ATI-5", "Gen-ATI-6"}, // fake
			"Champion Power Equipment": {"Gen-Cha-0", "Gen-Cha-1", "Gen-Cha-2"},                                                     // fake
			"Firman Power Equipment":   {"Gen-Fir-0", "Gen-Fir-1", "Gen-Fir-2", "Gen-Fir-3", "Gen-Fir-4"},                           // fake
		},
		"Air Handler": {
			"Carrier USA":          {"Air-Car-0", "Air-Car-1"}, // fake
			"Trane USA":            {"TAM9A0A24", "TAM9A0B30", "TAM9A0C36", "TAM7A0C42", "TAM7A0C48", "TAM7A0C60"},
			"York":                 {"Air-Yor-0", "Air-Yor-1", "Air-Yor-2", "Air-Yor-3", "Air-Yor-4"},                           // fake
			"Wolf GmbH":            {"Air-Wol-0", "Air-Wol-1", "Air-Wol-2", "Air-Wol-3", "Air-Wol-4", "Air-Wol-5", "Air-Wol-6"}, // fake
			"GEA Air Treatment":    {"Air-GEA-0"},                                                                               // fake
			"Sabiana":              {"Air-Sab-0", "Air-Sab-1", "Air-Sab-2", "Air-Sab-3", "Air-Sab-4", "Air-Sab-5", "Air-Sab-6"}, // fake
			"BPS Clima":            {"Air-BPS-0", "Air-BPS-1", "Air-BPS-2", "Air-BPS-3", "Air-BPS-4"},                           // fake
			"Goodman":              {"Air-Goo-0", "Air-Goo-1", "Air-Goo-2", "Air-Goo-3", "Air-Goo-4", "Air-Goo-5", "Air-Goo-6"}, // fake
			"Liebert":              {"Air-Lie-0", "Air-Lie-1", "Air-Lie-2", "Air-Lie-3", "Air-Lie-4", "Air-Lie-5", "Air-Lie-6"}, // fake
			"Nortek Air Solutions": {"Air-Nor-0", "Air-Nor-1"},                                                                  // fake
		},
		"Chiller": {
			"Cold Shot Chillers":               {"ACWC-24-QST", "ACWC-36-QST", "ACWC-60-QST", "ACWC-90-QST", "ACWC120-QST", "ACWC-150-EST", "ACWC-180-EST", "ACWC-240-EST", "ACWC-300-GCST", "ACWC-360-GCST", "ACWC-480-GCST", "ACWC-600-GCST", "ACWC-720-GCST", "ACWC-840-GCST", "ACWC-960-GCST", "ACWC-1080-GCST", "ACWC-1200-GCST"},
			"Thermal Care Inc":                 {"TSEW10S", "TSEW15S", "TSEW20S", "TSEW25S", "TSEW30S", "TSEW40S", "TSEW50S", "TSEW60S", "TSEW70S", "TSEW80S", "TSEW100S", "TSEW120S"},
			"Glen Dimplex Thermal Solutions":   {"SVI-3000-M", "SVI-5000-M", "SVI-7500-M", "SVI-10000-M", "SVI-15000-M", "SVI-20000-M", "SVO-2000-M", "SVO-3000-M", "SVO-5000-M", "SVO-7500-M", "SVO-10000-M", "SVO-15000-M", "SVO-20000-M"},
			"Chillermen":                       {"CMI20", "CMI40", "CMI60", "CMI80", "CMI100", "CMI120", "CMO20", "CMO40", "CMO60", "CMO80", "CMO100", "CMO120"},
			"Thermonics":                       {"W-40-1100", "W-40-2000", "W-40-3100", "W-60-2800", "W-60-3100", "W-60-4700", "W-80-600", "W-80-1800", "W-80-2500", "A-40-1100", "A-40-1900", "A-40-2700", "A-60-2700", "A-60-3000", "A-60-4600", "A-80-500", "A-80-1700", "A-80-2400"},
			"SINGLE Temperature Controls":      {"STWS-20-I", "STWS-30-I", "STWS-40-I", "STWS50-I", "STWS-60-I", "STWS-80-I", "STWS-100-I", "STWS-120-I", "STWS-20-O", "STWS-30-O", "STWS-40-O", "STWS-50-O", "STWS-60-O", "STWS-80-O", "STWS-100-O", "STWS-120-O"},
			"Dry Coolers":                      {"OCWC-20", "OCWC-60", "OCWC-80", "OCWC-100", "OCWC-120", "OCAC-20", "OCAC-60", "OCAC-80", "OCAC-100", "OCAC-120"},
			"Advance Industrial Refrigeration": {"CA-PW-100", "CA-SW-100", "CA-TW-100", "CA-CW-100", "CA-SA-100", "CA-TA-100", "CA-CA-100"},
			"Data Aire":                        {"gFORCE-ULTRA-80", "gFORCE-ULTRA-100", "gFORCE-ULTRA-120", "gFORCE-DX-80", "gFORCE-DX-100", "gFORCE-DX-120", "gFORCE-GT-80", "gFORCE-GT-100", "gFORCE-GT-120"},
		},
		"UPS": {
			"APC":        {"GVSUPS10KB4FS", "GVSUPS10KB2FS", "GVSUPS100KB5GS", "G5TUPS100", "G5TUPS40", "GUPXC100FS", "GUPXC100GFDIS"},
			"Mitsubishi": {"9900CX", "9900D", "9900AEGIS", "7011B"},
			"Eaton":      {"9PX700RT", "9PX1000RT", "9PX1000GRT", "5SC500", "5SC750", "5SC1000"},
			"Schneider":  {"E3MUPS100KHS", "E3MUPS120KHS", "E3MUPS200KHS"},
			"Siemens":    {"SITOP UPS5000", "SITOP UPS 501S", "SITOP PSU6200"},
		},
	},
	"Communications": {
		"Router": {
			"Meraki":  {"MX64-HW", "MR33-HW", "", "", "", "", "", "", ""},
			"Juniper": {"MX960", "MX240", "MX480", "MX2020", "MX10016", "MX10003", "MX2008", "MX10008"},
			"Cisco":   {"NCS 5001", "NCS 5002", "NCS 5011", "NCS 5501", "NCS 5501-SE", "NCS 5502", "NCS 5502-SE", "NCS 5516"},
			"Asus":    {"ROG Rapture GT-AC5300", "RT-AC5300", "ROG Rapture GT-AC11000", "RT-AX88U", "RT-N12+"},
		},
		"Server": {
			"Dell":    {"1300", "1400SC", "1500SC", "1550", "1600SC", "1650", "1655MC", "1750", "1800", "1850", "1855", "1900", "1950", "1955", "2100", "2200", "2300", "2400", "2450", "2500", "2500SC", "2550", "2600", "2650", "2800", "2850", "2900", "2950", "2970", "300", "300SC", "3250", "350", "400SC", "4100", "4300", "4350", "4400", "4600", "500SC", "600SC", "6100", "6300", "6350", "6400", "6450", "650", "6600", "6650", "6800", "6850", "6950", "700", "7150", "7250", "750", "800", "830", "840", "8450", "850", "860", "C1100", "C2100", "C410X", "C4130", "C4140", "C5000", "C5125", "C5220", "C5230", "C6100", "C6105", "C6145", "C6220", "C6220 II", "C6300", "C6320", "C6320p", "C6400", "C6420", "C8000", "C8220", "C8220 II", "C8220x", "C8220xd", "FC430", "FC630", "FC640", "FC830", "FD332", "FM120x4 (for PE FX2/FX2s)", "M420", "M520", "M520 (for PE VRTX)", "M600", "M605", "M610", "M610x", "M620", "M620 (for PE VRTX)", "M630", "M630 (for PE VRTX)", "M640", "M640 (for PE VRTX)", "M710", "M710HD", "M805", "M820", "M820 (for PE VRTX)", "M830", "M830 (for PE VRTX)", "M905", "M910", "M915", "MX5016s", "MX740c", "MX840c", "R200", "R210", "R210 II", "R220", "R230", "R240", "R300", "R310", "R320", "R330", "R340", "R410", "R415", "R420", "R420xr", "R430", "R440", "R510", "R515", "R520", "R530", "R530xd", "R540", "R610", "R620", "R630", "R640", "R6415", "R710", "R715", "R720", "R720xd", "R730", "R730xd", "R740", "R740xd", "R740xd2", "R7415", "R7425", "R805", "R810", "R815", "R820", "R830", "R840", "R900", "R905", "R910", "R920", "R930", "R940", "R940xa", "SC 400", "SC 420", "SC 430", "SC 440", "SC1420", "SC1425", "SC1430", "SC1435", "T100", "T105", "T110", "T110 II", "T130", "T140", "T20", "T30", "T300", "T310", "T320", "T330", "T340", "T410", "T420", "T430", "T440", "T605", "T610", "T620", "T630", "T640", "T710"},
			"IBM":     {"004", "00M", "014", "015", "017", "024", "084", "106", "112_212", "112_312", "124_224", "124_324", "12F", "12G", "12N", "12P", "12S", "1U3", "212", "212_312", "214_216", "21A", "21L", "224", "224_324", "22A", "22C", "22H", "22L", "22N", "22P", "22X", "2396LFA_2836LF8", "2397LFA_2836LF8", "2397LFA_2837LF8", "2398LFA_2836LF8", "2398LFA_2837LF8", "2398LFA_2838LF8", "2399LFA_2836LF8", "2399LFA_2837LF8", "2399LFA_2838LF8", "2399LFA_2839LF8", "2421961_2831985", "2421961_2831986", "2421961_283198E", "242196E_283185E", "242196E_283186E", "242196E_283198E", "2422961_2831985", "2422961_2831986", "2422961_283198E", "2422961_283298E", "242296E_283185E", "242296E_283186E", "242296E_283198E", "242296E_283298E", "2423961_2831985", "2423961_2831986", "2423961_283198E", "2423961_283298E", "2423961_283398E", "242396E_283185E", "242396E_283186E", "242396E_283198E", "242396E_283298E", "242396E_283398E", "2424961_2831985", "2424961_2831986", "2424961_283198E", "2424961_283298E", "2424961_283398E", "2424961_283498E", "242496E_283185E", "242496E_283186E", "242496E_283198E", "242496E_283298E", "242496E_283398E", "242496E_283498E", "24F", "24G", "25B", "25M", "2H2", "2H2_3H2", "2H4", "2H4_3H4", "2YR", "312", "324", "3H2", "3H4", "3YR", "416", "418", "41A", "420", "424", "425", "426", "42A", "42H", "430", "435", "436", "452", "4H4", "4YR", "520", "524_624", "525", "526", "530", "535", "536", "550", "551", "552", "55E_60E", "55F_60F", "55G_60G", "5YR", "60E", "60F", "60G", "60G_60F", "60S", "624", "631_632", "650", "651", "652", "6M2", "700", "724", "816", "824", "84E", "85E", "86E", "88E", "9119MHE_9080M9S", "9119MME_9080M9S", "92E_9AE", "92F", "92G", "931_941", "931_9B2", "932_941", "932_9B2", "94Y", "95E_96E", "95X", "983", "984", "985", "986", "988", "98B", "98E", "98F", "993", "994", "996", "9AE_92E", "A00", "A01", "A10", "A11_A21", "A6P", "A9F", "AC5", "ACL", "ACN", "ACP", "ACQ", "ACR", "AE1", "AE2", "AE3", "AE4", "AE5", "AE6", "AEA", "AEZ", "AF1", "AF4", "AF6", "AF7", "AF7_AF8", "AF8", "AFF", "AG8", "AIA", "AIE", "ALC", "ALS", "AMT", "AP1", "ARE", "AS1", "AS2", "AS3", "AS4", "AS5", "ASE", "B01", "B02", "B03", "B04", "B05", "B06", "B08", "B15", "B16", "B17", "B18", "B19", "B1A_E11", "B20", "B21", "B22", "B23", "B24", "B45", "BR1", "C10", "CB7", "CB8", "CBA", "CBB", "CBC", "CBD", "CBE", "CBF", "CBG", "CBH", "CD1", "CD3", "CD4", "CD5", "CD6", "CM1", "CMC", "CMF", "CMG", "CMH", "CMT", "CO1", "CO2", "CO3", "CO4", "CO5", "CP7", "CP8", "CR1", "CS1", "CS2", "CS3", "CS4", "CS5", "CS6", "CS7", "CS8", "CSA", "CSB", "CSM", "CT3", "CT5", "CT7", "CTA", "CTC", "CTF", "CTG", "CTH", "CY2", "CY3", "D22_D23", "D23_D53", "D25", "D52_D53", "D53_D23", "D55", "DAE", "DB1", "DB8", "DBA", "DBB", "DBC", "DBD", "DBE", "DBF", "DBG", "DBH", "DFH", "DME", "DP4", "DRP", "DS2", "E04", "E07_E08", "E07_EH7", "E08", "E08_60G", "E08_EH8", "E12", "E16", "E24", "E3A", "E85", "E92", "E96", "EAL", "EB7", "EB8", "EBA", "EBB", "EBC", "EBD", "EBE", "EBF", "EBG", "EBH", "ED0", "ED2", "EH7_EH8", "EH8", "EH8_60F", "EL5", "ELL", "ER6", "ES0", "ET7", "ET8", "ETA", "ETC", "ETF", "ETG", "ETH", "EV7", "EVA", "EVB", "EVC", "EVD", "EVE", "EVF", "EVG", "EVH", "EWM", "EXC", "EXF", "EXG", "EXH", "F00", "F04", "F05", "F06", "F08", "F12", "F16", "F17", "F18", "F19", "F20", "F21", "F22", "F23", "F24", "F25", "F3S", "F40", "F41", "F42", "F43", "F44", "F45", "F46", "F47", "F48", "F49", "F50", "F51", "F52", "F53", "F54", "F55", "F56", "F57", "F58", "F59", "F5A_F5C", "F60", "F61", "F62", "F63", "F64", "F6A", "F6A_F6C", "F6C", "F7A", "F7A_F7C", "F7C", "F8A", "F8C", "F8S", "F92", "F96", "FA1", "FA2", "FA3", "FAC", "FAD", "FAF", "FAG", "FAH", "FAJ", "FAL", "FAP", "FAQ", "FAR", "FAS", "FAT", "FAU", "FAV", "FAW", "FAX", "FAY", "FAZ", "FF8", "FM6", "FN1", "FNT", "FS1", "FS2", "FSJ", "FSK", "FSL", "FSM", "FSN", "FSP", "FSQ", "FSR", "FSS", "FST", "FSU", "FSV", "FSW", "FSX", "FSY", "FSZ", "G36", "G37", "G62", "G90", "G98", "GLP", "GTC", "GTG", "GTH", "GTW", "GTX", "GU6", "H23", "H24", "H25", "H37", "H39", "H42", "H5S", "H6S", "H7S", "H8S", "HAA", "HAB", "HAC", "HAD", "HAE", "HAF", "HAG", "HAH", "HAI", "HAM", "HAS", "HGO", "HMA", "HMB", "HMC", "HMV", "HMW", "IP1", "ITL", "IVP", "J06", "J07", "J08", "J09", "J10", "J11", "J12", "J13", "J15", "JS1", "L22", "L22_L23", "L23_L53", "L25", "L2C", "L3A", "L3S", "L52_L53", "L53_L23", "L55", "LEU", "LF8", "LFA", "LNK", "LNX", "LOP", "LP1", "LP2", "LP3", "LP4", "LP5", "LP6", "LP7", "LP8", "LP9", "LSF", "LT1", "LT2", "LT3", "LT4", "LT5", "LT6", "LT7", "LT8", "LT9", "M01", "M10", "M9S", "MAL", "MC1", "MC2", "MC3", "MC4", "MC5", "MC6", "MCA", "MCB", "MCC", "MCD", "MCE", "MCF", "MCG", "MCH", "MCJ", "MCK", "MCL", "MCM", "MCN", "MCP", "MCQ", "MCR", "MCS", "MCT", "MG1", "MHD", "MHE", "MHE_M9S", "MHU", "MM3", "MM4", "MM5", "MMA_MMX", "MMD", "MME_M9S", "MMU", "MMX", "MR9", "N64", "N96", "NKY", "NLV", "OEL", "OMF", "P03", "PAC", "PAE", "PAI", "PAV", "PC1", "PC2", "PCA", "PCS", "PD1", "PD2", "PDP", "PE3", "PEL", "PHN", "PLX", "PPM", "PSA", "PSE", "PST", "PT1", "PTB", "PTC", "PTL", "PVE", "PVL", "PVS", "PWO", "PXI", "QU1", "QU2", "QUB", "R04", "R16", "R18", "R42", "R50", "RB7", "RB8", "RBA", "RBB", "RBC", "RBD", "RBE", "RBF", "RBG", "RBH", "RDI", "RDS", "RDW", "REC", "REF", "REG", "REH", "RM7", "RMA", "RMB", "RMC", "RMD", "RME", "RMF", "RMG", "RMH", "RMZ", "RS1", "RS3", "S01", "S02", "S03", "S10", "S12", "S13", "S14", "S16", "S17", "S24", "S24_S54", "S25", "S2C", "S42", "S48", "S52", "S54_S24", "S55", "S5H", "S6H", "S7H", "S8H", "SA2", "SAP", "SAV", "SBV", "SCP", "SCV", "SDV", "SEU", "SEV", "SF1", "SF2", "SF3", "SF4", "SF5", "SF6", "SF7", "SF8", "SFV", "SGV", "SHV", "SKC", "SKL", "SKM", "SKP", "SLE", "SM1", "SM2", "SM3", "SM4", "SM5", "SM6", "SM7", "SM8", "SP3", "SPE", "SPM", "SPN", "SPO", "SPP", "SS1", "SSA", "SSB", "SSC", "SSE", "SSJ", "SSK", "SSL", "SSM", "SSN", "SSP", "SSQ", "SSR", "ST1", "STG", "SU3", "SV1", "SV2", "SV3", "SV4", "SV5", "SV6", "SV7", "SV8", "SV9", "SVA", "SVB", "SVC", "SVD", "SVE", "SVF", "SVG", "SVH", "SVJ", "SVK", "SVL", "SVM", "SVN", "SVP", "SVQ", "SVR", "SVS", "SVT", "SVU", "SVV", "SVW", "SVX", "SVY", "SVZ", "SWM", "SX3", "T00", "T12", "T24", "T32", "T42", "T48", "T96", "TAA", "TAB", "TAC", "TAD", "TAE", "TAF", "TAH", "TAI", "TBG", "TF4", "TL2", "TL3", "TL4", "TL5", "TL6", "TL7", "TL8", "TL9", "TR1", "U12", "U16", "U24", "U25", "U5B", "U7A", "U7B", "U7C", "UF3", "UF7", "UF8", "UG8", "UHB", "V12", "V24", "V42", "VC7", "VC8", "VCA", "VCB", "VCC", "VCD", "VCE", "VCS", "VE3", "VEC", "VED", "VF1", "VL3", "VM7", "VS3", "W28", "WDS", "WE2", "WE3", "WQE", "WQM", "WQS", "WQX", "X11", "X12", "X13", "X15", "X29", "X49", "X51", "X52", "X53", "X54", "X55", "X56", "X57", "X58", "XB7", "XB8", "XBA", "XBB", "XBC", "XBD", "XBE", "XBF", "XBG", "XBH", "XC1", "XC2", "XC3", "XC4", "XC5", "XC6", "XC7", "XC8", "XD8", "XDA", "XDB", "XDC", "XDD", "XDE", "XDF", "XDG", "XDH", "XSA", "XSB", "XT2", "XT7", "XTA", "XTB", "XTC", "XTF", "XTG", "XTH", "XW1", "XX1", "XX2", "XX3", "XX4", "XX5", "XX9", "XXC", "XXD", "XXE", "XXF", "XXG", "XXK", "XXL", "XXQ", "XXR", "XXS", "XXT", "YYB", "YYC", "YYE", "Z87", "ZZC", "ZZF", "ZZG", "ZZH"},
			"Cisco":   {"WAAS", "UCS", "ACE", "Mini", "B460_M4", "B420_M4", "B260_M4", "B200_M4", "B480_M5", "B200_M5", "C460_M4", "C240_M4", "C220_M4", "C480_M5", "C240_M5", "C220_M5", "6200", "6300"},
			"Lenovo":  {"TD340", "TD350", "RS140", "RD340", "RD440", "RD550", "RD650", "TS140", "TS440", "TD330", "TS130", "TS430", "TD200", "TD200x", "TD230", "TS200v", "RD220", "RD230", "RD240", "TD100", "TD100x", "TS200", "RS110", "RD120", "RD210", "RS210", "TS100"},
			"Acer":    {"Altos_T310", "Altos_T110", "Altos_T110_F4", "AT350_F3", "Altos_C100_F3", "AR780", "Altos_R380_F2", "Altos_R360_F2", "Altos_R320_F3", "AR580", "AW2000h", "AB7000_F3"},
			"Asus":    {"TS700-E8-RS8 V2", "TS700-E9-RS8", "RS720Q-E9-RS8-S", "RS700-E9-RS12", "RS720Q-E9-RS24-S", "RS720-E9-RS12-E", "RS100-E10-PI2", "RS100-E9-PI2", "ESC4000 G4", "ESC4000 G3", "RS500A-E9-RS4-U", "RS700A-E9-RS12", "ESC8000 G4/10G", "RS720-E9-RS24-E", "RS300-E10-RS4", "RS720A-E9-RS24-E", "RS520-E9-RS12-E", "RS300-E9-PS4", "RS700A-E9-RS4", "ESC8000 G4", "RS500A-E10-RS12U", "TS100-E10-PI4", "ESC4000 G4X", "ESC4000 DHD G4", "RS300-E9-RS4"},
			"Huawei":  {"9008", "9016", "9032", "E9000", "CH242", "CH226", "CH225", "CH222 ", "CH220", "CH140", "CH121", "CH140L", "CH121L", "9000", "6000", "RH8100", "RH5885H", "RH5885", "5288", "RH2288H", "RH2288", "RH1288", "X6800", "XH628", "XH622", "XH620", "X6000", "XH321"},
			"Fujitsu": {"TX1310_M1", "TX1320_M2", "TX1330_M2", "TX150_S8", "TX2540_M1", "TX2560_M2", "RX1330_M2", "RX2510_M2", "RX2520_M1", "RX2530_M2", "RX2540_M2", "RX2560_M2", "RX4770_M2", "BX400_S1", "BX600_S1", "BX600_S2", "BX600_S3", "BX900_S2", "BX2560_M2", "BX2580_M2", "BX2560_M2", "BX2580_M2", "CX400_M1", "CX400_S2", "CX420_S1", "CX2550_M2", "CX2570_M2", "CX250_S2", "CX270_S2", "CX272_S1"},
			"Intel":   {"R1208WFTYSR", "R1304WFTYSR", "R2208WFTZSR", "R2224WFTZSR", "R2308WFTZSR", "R2312WFTZSR", "M2098S-2U4N3", "M2099Q-2U4N6", "R1102S-1U4", "R2096S-2U8", "R2096S-2U8", "R2101S-2U24-Plus", "R2103S-1U10", "R2124Q-1U10", "R2122S-2U12", "W2001S-FULL", "W2002S-FULL", "T2097S-FULL8"},
			"NEC":     {"Express5800/R320f", "Express5800/R320g", "Express5800/R320g", "Express5800/R120g-1M", "Express5800/R120g-2M", "Express5800/R120g-2M"},
			"Oracle":  {"X8-2", "X8-2L", "X8-8", "M8-8", "M10-1", "M10-4S", "S7-2", "M12-1", "M12-2", "M12-2S"},
		},
		"BTS": {
			"CommScope":                   {"UAP", "UAP-X", "UAP-N25"},
			"Cisco":                       {"MFELX1-RF", "GLC-LH-SMD-RF", "SFP-GE-T-RF"},
			"Danphone":                    {"DCB9140", "DCM9140F"},
			"Microelectronics Technology": {"MCH-14801", "MRH-24605", "MCH-14801", "MCH-24713"},
			"Nortel":                      {"EL4512045", "AA1419043-E6"},
			"Powerwave Technologies":      {"Nexus FT 4000"},
			"Purcell Systems":             {"42RU", "37RU", "74RU"},
			"Sunwave":                     {"CrossFire HRU", "CrossFire NRU", "CrossFire AU", "Crossfire LRU"},
			"TriadRF Systems":             {"THPR"},
		},
		"CPE": {
			"Cisco":            {"1841", "2801", "2811", "2821", "2851", "3825", "3845"},
			"Motorola":         {"SB5101", "SB5101U", "SB6182", "SBV6220", "SB6121", "SBG6580", "SB6120"},
			"Arris":            {"CM820A", "DG1670A", "DG860A", "DG950A", "NVG468MQ", "SBG6400"},
			"Technicolor":      {"C2000T", "TC8117", "TC8305C", "TC8717"},
			"Netgear":          {"C6300", "C6300BD", "CG3000D", "CM1000", "CM400", "N600"},
			"Ubee":             {"UBC1302", "UBC1318", "UBC1322", "UBC1319", "UBC1303", "UBC1301", "EVW32C", "UBC1310", "UBC1307", "DVW32CB", "DDW36C"},
			"D-Link":           {"DCM 301"},
			"Huawei":           {"MT130U"},
			"SMC Networks":     {"SMCD3GNV3"},
			"Zoom Telephonics": {"5341", "5350", "5352"},
		},
	},
	"Transactions": {
		"POS": {
			"Square":      {"Square Terminal", "Square Register"},
			"Shopkeep":    {"POS-Sho-0", "POS-Sho-1", "POS-Sho-2", "POS-Sho-3", "POS-Sho-4", "POS-Sho-5"}, // fake
			"Springboard": {"B850", "B851", "125702"},
			"Lightspeed":  {"POS-Lig-0", "POS-Lig-1", "POS-Lig-2", "POS-Lig-3", "POS-Lig-4"},
			"Verifone":    {"E355", "E280", "E285", "Ruby2", "Topaz", "RubiCi"},
			"Revel":       {"M-S 16", "Cash Drawer", "Revel L Stand (Night)", "Revel C Stand", "Revel Stand Alone Kiosk", "Whozz Calling ID", "RevelGuard"},
			"Upserve":     {"POS-Ups-0", "POS-Ups-1", "POS-Ups-2", "POS-Ups-3", "POS-Ups-4", "POS-Ups-5"},
			"NCR":         {"PX10", "P1235", "P1532", "P1535", "XR7"},
			"GrOOV":       {"GrOOV One", "GrOOV One Plus", "GrOOV StoreFront", "GrOOV Terminal"},
			"Magtek":      {"DynaPro", "DynaPro Go", "DynaPro Mini"},
		},
	},
}

// domain,type,manufacturer,model
// var domains = map[string]map[string]map[string][]string{
// 	"Power": map[string]map[string][]string{
// 		"Breaker": map[string][]string{
// 			"GE":              []string{},
// 			"Westinghouse":    []string{},
// 			"Cutler-Hammer":   []string{},
// 			"Siemens":         []string{},
// 			"ABB":             []string{},
// 			"Federal Pacific": []string{},
// 			"Square D":        []string{},
// 			"PACS Industries": []string{},
// 		},
// 		"Generator": map[string][]string{
// 			"ABB":                      []string{},
// 			"Action":                   []string{},
// 			"Aggreko":                  []string{},
// 			"Atlas Copco":              []string{},
// 			"Caterpillar":              []string{},
// 			"Cummins":                  []string{},
// 			"Himoinso":                 []string{},
// 			"Ingersoll Rand":           []string{},
// 			"Kirloskar":                []string{},
// 			"Manlift Group":            []string{},
// 			"Generac":                  []string{},
// 			"Briggs & Stratton":        []string{},
// 			"Winco":                    []string{},
// 			"Westinghouse":             []string{},
// 			"DuroMax":                  []string{},
// 			"DuroStar":                 []string{},
// 			"ATIMA":                    []string{},
// 			"Champion Power Equipment": []string{},
// 			"Firman Power Equipment":   []string{},
// 		},
// 		"Air Handler": map[string][]string{
// 			"Carrier USA":          []string{},
// 			"Trane USA":            []string{},
// 			"York":                 []string{},
// 			"Wolf GmbH":            []string{},
// 			"GEA Air Treatment":    []string{},
// 			"Sabiana":              []string{},
// 			"BPS Clima":            []string{},
// 			"Goodman":              []string{},
// 			"Liebert":              []string{},
// 			"Nortek Air Solutions": []string{},
// 		},
// 		"Chiller": map[string][]string{
// 			"Cold Shot Chillers":               []string{},
// 			"Thermal Care Inc":                 []string{},
// 			"Glen Dimplex Thermal Solutions":   []string{},
// 			"Chillermem":                       []string{},
// 			"Thermonics":                       []string{},
// 			"SINGLE Temperature Controls":      []string{},
// 			"Dry Coolers":                      []string{},
// 			"Advance Industrial Refrigeration": []string{},
// 			"Data Aire":                        []string{},
// 		},
// 		"UPS": map[string][]string{
// 			"APC":        []string{},
// 			"Mitsubishi": []string{},
// 			"Eaton":      []string{},
// 			"Schneider":  []string{},
// 			"Siemens":    []string{},
// 		},
// 	},
// 	"Communications": map[string]map[string][]string{
// 		"Router": map[string][]string{
// 			"Meraki":  []string{},
// 			"Juniper": []string{},
// 			"Cisco":   []string{},
// 		},
// 		"Server": map[string][]string{
// 			"Dell":   []string{},
// 			"IBM":    []string{},
// 			"Cisco":  []string{},
// 			"Lenovo": []string{},
// 			"ACER":   []string{},
// 			"ASUS":   []string{},
// 			"Huawei": []string{},
// 			"Fuitsu": []string{},
// 			"Intel":  []string{},
// 			"NEC":    []string{},
// 			"Oracle": []string{},
// 		},
// 		"BTS": map[string][]string{
// 			"CommScope":                   []string{},
// 			"Cisco":                       []string{},
// 			"Danphone":                    []string{},
// 			"Microelectronics Technology": []string{},
// 			"Nortel":                      []string{},
// 			"Powerwave Technologies":      []string{},
// 			"Purcell Systems":             []string{},
// 			"Sunwave":                     []string{},
// 			"TriadRF Systems":             []string{},
// 		},
// 		"CPE": map[string][]string{
// 			"Cisco":            []string{},
// 			"Motorola":         []string{},
// 			"Arris":            []string{},
// 			"Technicolor":      []string{},
// 			"Netgear":          []string{},
// 			"Ubee":             []string{},
// 			"D-Link":           []string{},
// 			"Huawei":           []string{},
// 			"SMC Networks":     []string{},
// 			"Zoom Telephonics": []string{},
// 		},
// 	},
// 	"Transactions": map[string]map[string][]string{
// 		"POS": map[string][]string{
// 			"Square":      []string{},
// 			"Shopkepp":    []string{},
// 			"Springboard": []string{},
// 			"Lightspeed":  []string{},
// 			"Verifone":    []string{},
// 			"Revel":       []string{},
// 			"Upserve":     []string{},
// 			"NCR":         []string{},
// 			"GrOOV":       []string{},
// 			"Magtek":      []string{},
// 		},
// 	},
// }
