package datagen

import (
	"io"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/idk/datagen/gen"
)

// Ensure Merck implements interface.
var _ Sourcer = (*Merck)(nil)

// Merck implements Sourcer, and returns a very basic
// data set. It can be used as an Merck for writing
// additional custom Sourcers.
type Merck struct{}

// NewMerck returns a new instance of Merck.
func NewMerck(cfg SourceGeneratorConfig) Sourcer {
	return &Merck{}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (e *Merck) Source(cfg SourceConfig) idk.Source {
	src := &MerckSource{
		cur:   cfg.startFrom,
		endAt: cfg.endAt,
		rand:  rand.New(rand.NewSource(19)),
		schema: []idk.Field{
			idk.IDField{NameVal: "id"},                                                                    //0
			idk.StringField{NameVal: "image_location"},                                                    //1
			idk.StringField{NameVal: "image_tag"},                                                         //2
			idk.StringField{NameVal: "algorithm_name"},                                                    //3
			idk.StringField{NameVal: "start_time"},                                                        //4
			idk.StringField{NameVal: "analysis_region"},                                                   //5
			idk.IntField{NameVal: "total_cells", Min: intptr(1000), Max: intptr(1000000)},                 //6
			idk.IntField{NameVal: "dye_1_positive_cells", Min: intptr(0), Max: intptr(1000000)},           //7
			idk.IntField{NameVal: "dye_2_positive_cells", Min: intptr(0), Max: intptr(1000000)},           //8
			idk.IntField{NameVal: "dye_3_positive_cells", Min: intptr(0), Max: intptr(1000000)},           //9
			idk.IntField{NameVal: "dye_4_positive_cells", Min: intptr(0), Max: intptr(1000000)},           //10
			idk.IntField{NameVal: "dye_5_positive_cells", Min: intptr(0), Max: intptr(1000000)},           //11
			idk.IntField{NameVal: "dye_6_positive_cells", Min: intptr(0), Max: intptr(1000000)},           //12
			idk.IntField{NameVal: "dye_7_positive_cells", Min: intptr(0), Max: intptr(1000000)},           //13
			idk.IntField{NameVal: "dye_8_positive_cells", Min: intptr(0), Max: intptr(1000000)},           //14
			idk.IntField{NameVal: "dye_1_positive_nuclei_cells", Min: intptr(0), Max: intptr(1000000)},    //15
			idk.IntField{NameVal: "dye_2_positive_nuclei_cells", Min: intptr(0), Max: intptr(1000000)},    //16
			idk.IntField{NameVal: "dye_3_positive_nuclei_cells", Min: intptr(0), Max: intptr(1000000)},    //17
			idk.IntField{NameVal: "dye_4_positive_nuclei_cells", Min: intptr(0), Max: intptr(1000000)},    //18
			idk.IntField{NameVal: "dye_5_positive_nuclei_cells", Min: intptr(0), Max: intptr(1000000)},    //19
			idk.IntField{NameVal: "dye_6_positive_nuclei_cells", Min: intptr(0), Max: intptr(1000000)},    //20
			idk.IntField{NameVal: "dye_7_positive_nuclei_cells", Min: intptr(0), Max: intptr(1000000)},    //21
			idk.IntField{NameVal: "dye_8_positive_nuclei_cells", Min: intptr(0), Max: intptr(1000000)},    //22
			idk.IntField{NameVal: "negative_nuclei_cells", Min: intptr(0), Max: intptr(1000000)},          //23
			idk.IntField{NameVal: "dye_1_positive_cytoplasm_cells", Min: intptr(0), Max: intptr(1000000)}, //24
			idk.IntField{NameVal: "dye_2_positive_cytoplasm_cells", Min: intptr(0), Max: intptr(1000000)}, //25
			idk.IntField{NameVal: "dye_3_positive_cytoplasm_cells", Min: intptr(0), Max: intptr(1000000)}, //26
			idk.IntField{NameVal: "dye_4_positive_cytoplasm_cells", Min: intptr(0), Max: intptr(1000000)}, //27
			idk.IntField{NameVal: "dye_5_positive_cytoplasm_cells", Min: intptr(0), Max: intptr(1000000)}, //28
			idk.IntField{NameVal: "dye_6_positive_cytoplasm_cells", Min: intptr(0), Max: intptr(1000000)}, //29
			idk.IntField{NameVal: "dye_7_positive_cytoplasm_cells", Min: intptr(0), Max: intptr(1000000)}, //30
			idk.IntField{NameVal: "dye_8_positive_cytoplasm_cells", Min: intptr(0), Max: intptr(1000000)}, //31
			idk.IntField{NameVal: "negative_cytoplasm_cells", Min: intptr(0), Max: intptr(1000000)},       //32
			idk.IntField{NameVal: "dye_1_positive_membrane_cells", Min: intptr(0), Max: intptr(1000000)},  //33
			idk.IntField{NameVal: "dye_2_positive_membrane_cells", Min: intptr(0), Max: intptr(1000000)},  //34
			idk.IntField{NameVal: "dye_3_positive_membrane_cells", Min: intptr(0), Max: intptr(1000000)},  //35
			idk.IntField{NameVal: "dye_4_positive_membrane_cells", Min: intptr(0), Max: intptr(1000000)},  //36
			idk.IntField{NameVal: "dye_5_positive_membrane_cells", Min: intptr(0), Max: intptr(1000000)},  //37
			idk.IntField{NameVal: "dye_6_positive_membrane_cells", Min: intptr(0), Max: intptr(1000000)},  //38
			idk.IntField{NameVal: "dye_7_positive_membrane_cells", Min: intptr(0), Max: intptr(1000000)},  //39
			idk.IntField{NameVal: "dye_8_positive_membrane_cells", Min: intptr(0), Max: intptr(1000000)},  //40
			idk.IntField{NameVal: "negative_membrane_cells", Min: intptr(0), Max: intptr(1000000)},        //41
			idk.DecimalField{NameVal: "dye_1_per_positive_cells", Scale: 6},                               //42
			idk.DecimalField{NameVal: "dye_2_per_positive_cells", Scale: 6},                               //43
			idk.DecimalField{NameVal: "dye_3_per_positive_cells", Scale: 6},                               //44
			idk.DecimalField{NameVal: "dye_4_per_positive_cells", Scale: 6},                               //45
			idk.DecimalField{NameVal: "dye_5_per_positive_cells", Scale: 6},                               //46
			idk.DecimalField{NameVal: "dye_6_per_positive_cells", Scale: 6},                               //47
			idk.DecimalField{NameVal: "dye_7_per_positive_cells", Scale: 6},                               //48
			idk.DecimalField{NameVal: "dye_8_per_positive_cells", Scale: 6},                               //49
			idk.DecimalField{NameVal: "dye_1_per_positive_nuclei_cells", Scale: 6},                        //50
			idk.DecimalField{NameVal: "dye_2_per_positive_nuclei_cells", Scale: 6},                        //51
			idk.DecimalField{NameVal: "dye_3_per_positive_nuclei_cells", Scale: 6},                        //52
			idk.DecimalField{NameVal: "dye_4_per_positive_nuclei_cells", Scale: 6},                        //53
			idk.DecimalField{NameVal: "dye_5_per_positive_nuclei_cells", Scale: 6},                        //54
			idk.DecimalField{NameVal: "dye_6_per_positive_nuclei_cells", Scale: 6},                        //55
			idk.DecimalField{NameVal: "dye_7_per_positive_nuclei_cells", Scale: 6},                        //56
			idk.DecimalField{NameVal: "dye_8_per_positive_nuclei_cells", Scale: 6},                        //57
			idk.DecimalField{NameVal: "dye_1_per_positive_cytoplasm_cells", Scale: 6},                     //58
			idk.DecimalField{NameVal: "dye_2_per_positive_cytoplasm_cells", Scale: 6},                     //59
			idk.DecimalField{NameVal: "dye_3_per_positive_cytoplasm_cells", Scale: 6},                     //60
			idk.DecimalField{NameVal: "dye_4_per_positive_cytoplasm_cells", Scale: 6},                     //61
			idk.DecimalField{NameVal: "dye_5_per_positive_cytoplasm_cells", Scale: 6},                     //62
			idk.DecimalField{NameVal: "dye_6_per_positive_cytoplasm_cells", Scale: 6},                     //63
			idk.DecimalField{NameVal: "dye_7_per_positive_cytoplasm_cells", Scale: 6},                     //64
			idk.DecimalField{NameVal: "dye_8_per_positive_cytoplasm_cells", Scale: 6},                     //65
			idk.DecimalField{NameVal: "dye_1_per_positive_membrane_cells", Scale: 6},                      //66
			idk.DecimalField{NameVal: "dye_2_per_positive_membrane_cells", Scale: 6},                      //67
			idk.DecimalField{NameVal: "dye_3_per_positive_membrane_cells", Scale: 6},                      //68
			idk.DecimalField{NameVal: "dye_4_per_positive_membrane_cells", Scale: 6},                      //69
			idk.DecimalField{NameVal: "dye_5_per_positive_membrane_cells", Scale: 6},                      //70
			idk.DecimalField{NameVal: "dye_6_per_positive_membrane_cells", Scale: 6},                      //71
			idk.DecimalField{NameVal: "dye_7_per_positive_membrane_cells", Scale: 6},                      //72
			idk.DecimalField{NameVal: "dye_8_per_positive_membrane_cells", Scale: 6},                      //73
			idk.DecimalField{NameVal: "dye_1_positive_nuclei_avg_intensity", Scale: 6},                    //74
			idk.DecimalField{NameVal: "dye_1_positive_cytoplasm_avg_intensity", Scale: 6},                 //75
			idk.DecimalField{NameVal: "dye_1_positive_membrane_avg_intensity", Scale: 6},                  //76
			idk.DecimalField{NameVal: "dye_2_positive_nuclei_avg_intensity", Scale: 6},                    //77
			idk.DecimalField{NameVal: "dye_2_positive_cytoplasm_avg_intensity", Scale: 6},                 //78
			idk.DecimalField{NameVal: "dye_2_positive_membrane_avg_intensity", Scale: 6},                  //79
			idk.DecimalField{NameVal: "dye_3_positive_nuclei_avg_intensity", Scale: 6},                    //80
			idk.DecimalField{NameVal: "dye_3_positive_cytoplasm_avg_intensity", Scale: 6},                 //81
			idk.DecimalField{NameVal: "dye_3_positive_membrane_avg_intensity", Scale: 6},                  //82
			idk.DecimalField{NameVal: "dye_4_positive_nuclei_avg_intensity", Scale: 6},                    //83
			idk.DecimalField{NameVal: "dye_4_positive_cytoplasm_avg_intensity", Scale: 6},                 //84
			idk.DecimalField{NameVal: "dye_4_positive_membrane_avg_intensity", Scale: 6},                  //85
			idk.DecimalField{NameVal: "dye_5_positive_nuclei_avg_intensity", Scale: 6},                    //86
			idk.DecimalField{NameVal: "dye_5_positive_cytoplasm_avg_intensity", Scale: 6},                 //87
			idk.DecimalField{NameVal: "dye_5_positive_membrane_avg_intensity", Scale: 6},                  //88
			idk.DecimalField{NameVal: "dye_6_positive_nuclei_avg_intensity", Scale: 6},                    //89
			idk.DecimalField{NameVal: "dye_6_positive_cytoplasm_avg_intensity", Scale: 6},                 //90
			idk.DecimalField{NameVal: "dye_6_positive_membrane_avg_intensity", Scale: 6},                  //91
			idk.DecimalField{NameVal: "dye_7_positive_nuclei_avg_intensity", Scale: 6},                    //92
			idk.DecimalField{NameVal: "dye_7_positive_cytoplasm_avg_intensity", Scale: 6},                 //93
			idk.DecimalField{NameVal: "dye_7_positive_membrane_avg_intensity", Scale: 6},                  //94
			idk.DecimalField{NameVal: "dye_8_positive_nuclei_avg_intensity", Scale: 6},                    //95
			idk.DecimalField{NameVal: "dye_8_positive_cytoplasm_avg_intensity", Scale: 6},                 //96
			idk.DecimalField{NameVal: "dye_8_positive_membrane_avg_intensity", Scale: 6},                  //97
			idk.DecimalField{NameVal: "avg_nucleus_area_um2", Scale: 6},                                   //98
			idk.DecimalField{NameVal: "avg_cytoplasm_area_um2", Scale: 6},                                 //99
			idk.DecimalField{NameVal: "avg_cell_area_um2", Scale: 6},                                      //100
			idk.IntField{NameVal: "area_analyzed_um2"},                                                    //101
			idk.IntField{NameVal: "image_zoom"},                                                           //102
			idk.BoolField{NameVal: "classifier"},                                                          //103
			idk.StringField{NameVal: "class_list"},                                                        //104
			idk.BoolField{NameVal: "classify_registered"},                                                 //105
			idk.StringField{NameVal: "classifier_output_type"},                                            //106
			idk.StringField{NameVal: "dye_1"},                                                             //107
			idk.BoolField{NameVal: "dye_1_membrane_segmentation"},                                         //108
			idk.DecimalField{NameVal: "dye_1_nucleus_weight", Scale: 3},                                   //109
			idk.DecimalField{NameVal: "dye_1_nucleus_positive_threshold", Scale: 3},                       //110
			idk.DecimalField{NameVal: "dye_1_cytoplasm_positive_threshold", Scale: 3},                     //111
			idk.DecimalField{NameVal: "dye_1_membrane_positive_threshold", Scale: 3},                      //112
			idk.StringField{NameVal: "dye_1_nucleus_mask"},                                                //113
			idk.StringField{NameVal: "dye_1_cytoplasm_mask"},                                              //114
			idk.StringField{NameVal: "dye_1_membrane_mask"},                                               //115
			idk.StringField{NameVal: "dye_2"},                                                             //116
			idk.BoolField{NameVal: "dye_2_membrane_segmentation"},                                         //117
			idk.DecimalField{NameVal: "dye_2_nucleus_weight", Scale: 3},                                   //118
			idk.DecimalField{NameVal: "dye_2_nucleus_positive_threshold", Scale: 3},                       //119
			idk.DecimalField{NameVal: "dye_2_cytoplasm_positive_threshold", Scale: 3},                     //120
			idk.DecimalField{NameVal: "dye_2_membrane_positive_threshold", Scale: 3},                      //121
			idk.StringField{NameVal: "dye_2_nucleus_mask"},                                                //122
			idk.StringField{NameVal: "dye_2_cytoplasm_mask"},                                              //123
			idk.StringField{NameVal: "dye_2_membrane_mask"},                                               //124
			idk.StringField{NameVal: "dye_3"},                                                             //125
			idk.BoolField{NameVal: "dye_3_membrane_segmentation"},                                         //126
			idk.DecimalField{NameVal: "dye_3_nucleus_weight", Scale: 3},                                   //127
			idk.DecimalField{NameVal: "dye_3_nucleus_positive_threshold", Scale: 3},                       //128
			idk.DecimalField{NameVal: "dye_3_cytoplasm_positive_threshold", Scale: 3},                     //129
			idk.DecimalField{NameVal: "dye_3_membrane_positive_threshold", Scale: 3},                      //130
			idk.StringField{NameVal: "dye_3_nucleus_mask"},                                                //131
			idk.StringField{NameVal: "dye_3_cytoplasm_mask"},                                              //132
			idk.StringField{NameVal: "dye_3_membrane_mask"},                                               //133
			idk.StringField{NameVal: "dye_4"},                                                             //134
			idk.BoolField{NameVal: "dye_4_membrane_segmentation"},                                         //135
			idk.DecimalField{NameVal: "dye_4_nucleus_weight", Scale: 3},                                   //136
			idk.DecimalField{NameVal: "dye_4_nucleus_positive_threshold", Scale: 3},                       //137
			idk.DecimalField{NameVal: "dye_4_cytoplasm_positive_threshold", Scale: 3},                     //138
			idk.DecimalField{NameVal: "dye_4_membrane_positive_threshold", Scale: 3},                      //139
			idk.StringField{NameVal: "dye_4_nucleus_mask"},                                                //140
			idk.StringField{NameVal: "dye_4_cytoplasm_mask"},                                              //141
			idk.StringField{NameVal: "dye_4_membrane_mask"},                                               //142
			idk.StringField{NameVal: "dye_5"},                                                             //143
			idk.BoolField{NameVal: "dye_5_membrane_segmentation"},                                         //144
			idk.DecimalField{NameVal: "dye_5_nucleus_weight", Scale: 3},                                   //145
			idk.DecimalField{NameVal: "dye_5_nucleus_positive_threshold", Scale: 3},                       //146
			idk.DecimalField{NameVal: "dye_5_cytoplasm_positive_threshold", Scale: 3},                     //147
			idk.DecimalField{NameVal: "dye_5_membrane_positive_threshold", Scale: 3},                      //148
			idk.StringField{NameVal: "dye_5_nucleus_mask"},                                                //149
			idk.StringField{NameVal: "dye_5_cytoplasm_mask"},                                              //150
			idk.StringField{NameVal: "dye_5_membrane_mask"},                                               //151
			idk.StringField{NameVal: "dye_6"},                                                             //152
			idk.BoolField{NameVal: "dye_6_membrane_segmentation"},                                         //153
			idk.DecimalField{NameVal: "dye_6_nucleus_weight", Scale: 3},                                   //154
			idk.DecimalField{NameVal: "dye_6_nucleus_positive_threshold", Scale: 3},                       //155
			idk.DecimalField{NameVal: "dye_6_cytoplasm_positive_threshold", Scale: 3},                     //156
			idk.DecimalField{NameVal: "dye_6_membrane_positive_threshold", Scale: 3},                      //157
			idk.StringField{NameVal: "dye_6_nucleus_mask"},                                                //158
			idk.StringField{NameVal: "dye_6_cytoplasm_mask"},                                              //159
			idk.StringField{NameVal: "dye_6_membrane_mask"},                                               //160
			idk.StringField{NameVal: "dye_7"},                                                             //161
			idk.BoolField{NameVal: "dye_7_membrane_segmentation"},                                         //162
			idk.DecimalField{NameVal: "dye_7_nucleus_weight", Scale: 3},                                   //163
			idk.DecimalField{NameVal: "dye_7_nucleus_positive_threshold", Scale: 3},                       //164
			idk.DecimalField{NameVal: "dye_7_cytoplasm_positive_threshold", Scale: 3},                     //165
			idk.DecimalField{NameVal: "dye_7_membrane_positive_threshold", Scale: 3},                      //166
			idk.StringField{NameVal: "dye_7_nucleus_mask"},                                                //167
			idk.StringField{NameVal: "dye_7_cytoplasm_mask"},                                              //168
			idk.StringField{NameVal: "dye_7_membrane_mask"},                                               //169
			idk.StringField{NameVal: "dye_8"},                                                             //170
			idk.BoolField{NameVal: "dye_8_membrane_segmentation"},                                         //171
			idk.DecimalField{NameVal: "dye_8_nucleus_weight", Scale: 3},                                   //172
			idk.DecimalField{NameVal: "dye_8_nucleus_positive_threshold", Scale: 3},                       //173
			idk.DecimalField{NameVal: "dye_8_cytoplasm_positive_threshold", Scale: 3},                     //174
			idk.DecimalField{NameVal: "dye_8_membrane_positive_threshold", Scale: 3},                      //175
			idk.StringField{NameVal: "dye_8_nucleus_mask"},                                                //176
			idk.StringField{NameVal: "dye_8_cytoplasm_mask"},                                              //177
			idk.StringField{NameVal: "dye_8_membrane_mask"},                                               //178
			idk.DecimalField{NameVal: "nuclear_contrast_threshold", Scale: 3},                             //179
			idk.DecimalField{NameVal: "minimum_nuclear_intensity", Scale: 3},                              //180
			idk.DecimalField{NameVal: "nuclear_segmentation_aggressiveness", Scale: 3},                    //181
			idk.DecimalField{NameVal: "fill_nuclear_holes", Scale: 3},                                     //182
			idk.DecimalField{NameVal: "minimum_nuclear_size", Scale: 3},                                   //183
			idk.DecimalField{NameVal: "maximum_nuclear_size", Scale: 3},                                   //184
			idk.DecimalField{NameVal: "minimum_nuclear_roundness", Scale: 3},                              //185
			idk.DecimalField{NameVal: "maximum_cytoplasm_radius", Scale: 3},                               //186
			idk.DecimalField{NameVal: "membrane_segmentation_agressiveness", Scale: 3},                    //187
			idk.DecimalField{NameVal: "minimum_cell_size", Scale: 3},                                      //188
			idk.DecimalField{NameVal: "maximum_cell_size", Scale: 3},                                      //189
			idk.StringField{NameVal: "output_image"},                                                      //190
			idk.DecimalField{NameVal: "store_object_cell_data"},                                           //191
			idk.DecimalField{NameVal: "nuclear_size_minimum", Scale: 3},                                   //192
			idk.DecimalField{NameVal: "nuclear_size_maximum", Scale: 3},                                   //193
			idk.DecimalField{NameVal: "cell_size_minimum", Scale: 3},                                      //194
			idk.DecimalField{NameVal: "cell_size_maximum", Scale: 3},                                      //195
			idk.IDField{NameVal: "subject_id"},                                                            //196
			idk.StringField{NameVal: "su_species"},                                                        //197
			idk.StringField{NameVal: "su_sex"},                                                            //198
			idk.StringField{NameVal: "su_date_of_birth"},                                                  //199
			idk.IDField{NameVal: "case_id"},                                                               //200
			idk.IntField{NameVal: "c_age"},                                                                //201
			idk.IntField{NameVal: "c_weight"},                                                             //202
			idk.IDArrayField{NameVal: "procedure_id"},                                                     //203
			idk.StringArrayField{NameVal: "p_name"},                                                       //204
			idk.StringArrayField{NameVal: "p_description"},                                                //205
			idk.IntField{NameVal: "p_ontology_code"},                                                      //206
			idk.IDField{NameVal: "diagnosis_id"},                                                          //207
			idk.StringField{NameVal: "dx_comments"},                                                       //208
			idk.IDField{NameVal: "disease_id"},                                                            //209
			idk.StringField{NameVal: "dz_disease_name"},                                                   //210
			idk.StringField{NameVal: "dz_etiology"},                                                       //211
			idk.StringField{NameVal: "dz_morphology"},                                                     //212
			idk.IntField{NameVal: "dz_ontology_code"},                                                     //213
			idk.IDField{NameVal: "specimen_id"},                                                           //214
			idk.StringField{NameVal: "sp_tissue_type"},                                                    //215
			idk.IntField{NameVal: "sp_sample_volume"},                                                     //216
			idk.StringField{NameVal: "sp_sample_location"},                                                //217
		},
	}
	src.Z = gen.New(gen.OptGenSeed(cfg.seed))
	src.algZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(alg))-1)
	src.maskZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(mask))-1)
	src.analysisregionZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(analysisregion))-1)
	src.classifierTypeZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(classifierType))-1)
	src.classListZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(classList))-1)
	src.outputTypeZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(outputType))-1)
	src.dyeZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(dye))-1)
	src.outputImageZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(outputImage))-1)
	src.speciesZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(species))-1)
	src.sexZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(sex))-1)
	src.commentsZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(comments))-1)
	src.locationTissueZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(locationTissue))-1)
	src.tissuetypeZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(tissuetype))-1)
	src.etiologyZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(etiology))-1)
	src.morphologyZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(morphology))-1)
	src.procedures_descZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(procedures_desc))-1)
	src.diseaseZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(disease))-1)
	src.record = make([]interface{}, len(src.schema))
	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (e *Merck) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (e *Merck) DefaultEndAt() uint64 {
	return 100
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (e *Merck) Info() string {
	return "Generates data representative of Merck's pharmaceutical operations."
}

var alg = []string{"NSCLC T", "TC1_28", "NSCLC-LY-21-LA", "NSCLC-LY-22-LA", "NSCLC-LY-23-LA", "NSCLC-LY-24-LA", "NSCLC-LY-25-LA", "NSCLC-LY-37-LA", "NSCLC-LY-38-LA", "NSCLC-LY-39-LA", "NSCLC-LY-40-LA", "NSCLC-LY-41-LA", "NSCLC-LY-42-LA", "NSCLC-LY-43-LA", "NSCLC-LY-44-LA", "NSCLC-LY-45-LA", "NSCLC-LY-46-LA", "NSCLC-LY-47-LA", "NSCLC-LY-48-LA", "NSCLC-LY-59-LA", "NSCLC-LY-60-LA"}

var mask = []string{"not set", "OR Negative", "OR Positive"}

var species = []string{"Homo sapiens", "Rattus", "Sus scrofa"}

var sex = []string{"M", "F", "U"}

var etiology = []string{"Smoking", "Sun light", "Diet Coke", "Sneezing", "Open Cut"}

var morphology = []string{"Lungs", "Skin", "Blood", "Liver", "Stomach"}

var locationTissue = []string{"US", "UK", "CAN", "IND"}

var disease = []string{"Cancer", "Diabetes", "Thyroid", "Leukemia"}

var tissuetype = []string{"MUSC", "NERV", "EPIT", "CONN"}

var procedures_desc = []string{"Cut muscle", "Stich muscle", "Remove skin", "Add oil", "Triple bypass ", "Tape bone", "Remove organ", "Unplug", "Sedate", "Apply bandaid"}

var comments = []string{"aliquip ex ea commodo consequat", "cupidatat non proident, sunt in culpa", "Lorem ipsum dolor sit amet, consectetur", "Excepteur sint occaecat cupidatat non proident", "sunt in culpa qui officia", "deserunt mollit anim id est laborum", "Duis aute irure dolor"}

var analysisregion = []string{"Layer 1", "Layer 2", "Layer 3", "Layer 4", "Layer 5", "Layer 6", "Layer 7", "Layer 8", "Layer 9", "Layer 10"}

var classifierType = []string{"A", "B", "C", "D"}

var classList = []string{"1", "2", "3", "4"}

var outputType = []string{"MASK", "MASK-RCNN", "MASK-NMS"}

var dye = []string{"DAPI", "Autofluorescence", "Opal 690", "Opal 650", "Opal 620", "Opal 570", "Opal 540", "Opal 520"}

var outputImage = []string{"Dye 1", "Dye 2", "Dye 3", "Dye 4", "Dye 5", "Dye 6", "Dye 7", "Dye 8"}

// Ensure MerckSource implements interface.
var _ idk.Source = (*MerckSource)(nil)

// MerckSource is an instance of a source generated
// by the Sourcer implementation Merck.
type MerckSource struct {
	cur                 uint64
	endAt               uint64
	Z                   *gen.Gen
	rand                *rand.Rand
	algZipf             *rand.Zipf
	maskZipf            *rand.Zipf
	analysisregionZipf  *rand.Zipf
	classListZipf       *rand.Zipf
	classifierTypeZipf  *rand.Zipf
	outputTypeZipf      *rand.Zipf
	dyeZipf             *rand.Zipf
	outputImageZipf     *rand.Zipf
	speciesZipf         *rand.Zipf
	sexZipf             *rand.Zipf
	commentsZipf        *rand.Zipf
	tissuetypeZipf      *rand.Zipf
	locationTissueZipf  *rand.Zipf
	morphologyZipf      *rand.Zipf
	etiologyZipf        *rand.Zipf
	procedures_descZipf *rand.Zipf
	diseaseZipf         *rand.Zipf
	schema              []idk.Field
	record              record
}

// Record implements idk.Source.
func (e *MerckSource) Record() (idk.Record, error) {
	if e.cur >= e.endAt {
		return nil, io.EOF
	}
	imageName := e.String(5) + ".tif"
	imageLocation := "\\myserver\\" + imageName

	// ID allocation
	total_records := e.endAt - 1
	subjectID := int(math.Ceil(float64(total_records) / float64(128)))
	caseID := int(math.Ceil(float64(total_records) / float64(32)))
	specimenID := int(math.Ceil(float64(total_records) / float64(8)))

	//<-- CELLS BEGIN -->
	dye_1_p := e.generate10(1000000, 0)
	dye_2_p := e.generate10(1000000, 0)
	dye_3_p := e.generate10(1000000, 0)
	dye_4_p := e.generate10(1000000, 0)
	dye_5_p := e.generate10(1000000, 0)
	dye_6_p := e.generate10(1000000, 0)
	dye_7_p := e.generate10(1000000, 0)
	dye_8_p := e.generate10(1000000, 0)
	dye_1_p_n := e.generate10(1000000, 0)
	dye_2_p_n := e.generate10(1000000, 0)
	dye_3_p_n := e.generate10(1000000, 0)
	dye_4_p_n := e.generate10(1000000, 0)
	dye_5_p_n := e.generate10(1000000, 0)
	dye_6_p_n := e.generate10(1000000, 0)
	dye_7_p_n := e.generate10(1000000, 0)
	dye_8_p_n := e.generate10(1000000, 0)
	dye_1_p_c := e.generate50(1000000, 0)
	dye_2_p_c := e.generate50(1000000, 0)
	dye_3_p_c := e.generate50(1000000, 0)
	dye_4_p_c := e.generate50(1000000, 0)
	dye_5_p_c := e.generate50(1000000, 0)
	dye_6_p_c := e.generate50(1000000, 0)
	dye_7_p_c := e.generate50(1000000, 0)
	dye_8_p_c := e.generate50(1000000, 0)
	dye_1_p_m := e.generate50(1000000, 0)
	dye_2_p_m := e.generate50(1000000, 0)
	dye_3_p_m := e.generate50(1000000, 0)
	dye_4_p_m := e.generate50(1000000, 0)
	dye_5_p_m := e.generate50(1000000, 0)
	dye_6_p_m := e.generate50(1000000, 0)
	dye_7_p_m := e.generate50(1000000, 0)
	dye_8_p_m := e.generate50(1000000, 0)
	total_cells := e.generateRandomInt(1000000, 1000)
	//<-- CELLS END -->

	e.record[0] = e.cur
	e.record[1] = imageLocation
	e.record[2] = imageName
	e.record[3] = alg[e.algZipf.Uint64()]
	e.record[4] = startTime.Add(time.Duration(e.Z.R.Int63n(int64(timeSpan))))
	e.record[5] = analysisregion[e.analysisregionZipf.Uint64()]
	e.record[6] = total_cells
	e.record[7] = dye_1_p
	e.record[8] = dye_2_p
	e.record[9] = dye_3_p
	e.record[10] = dye_4_p
	e.record[11] = dye_5_p
	e.record[12] = dye_6_p
	e.record[13] = dye_7_p
	e.record[14] = dye_8_p
	e.record[15] = dye_1_p_n
	e.record[16] = dye_2_p_n
	e.record[17] = dye_3_p_n
	e.record[18] = dye_4_p_n
	e.record[19] = dye_5_p_n
	e.record[20] = dye_6_p_n
	e.record[21] = dye_7_p_n
	e.record[22] = dye_8_p_n
	e.record[23] = e.generate50(1000000, 0)
	e.record[24] = dye_1_p_c
	e.record[25] = dye_2_p_c
	e.record[26] = dye_3_p_c
	e.record[27] = dye_4_p_c
	e.record[28] = dye_5_p_c
	e.record[29] = dye_6_p_c
	e.record[30] = dye_7_p_c
	e.record[31] = dye_8_p_c
	e.record[32] = e.generate50(1000000, 0)
	e.record[33] = dye_1_p_m
	e.record[34] = dye_2_p_m
	e.record[35] = dye_3_p_m
	e.record[36] = dye_4_p_m
	e.record[37] = dye_5_p_m
	e.record[38] = dye_6_p_m
	e.record[39] = dye_7_p_m
	e.record[40] = dye_8_p_m
	e.record[41] = e.generate50(1000000, 0)
	e.record[42] = float64(dye_1_p) / float64(total_cells)
	e.record[43] = float64(dye_3_p) / float64(total_cells)
	e.record[44] = float64(dye_3_p) / float64(total_cells)
	e.record[45] = float64(dye_4_p) / float64(total_cells)
	e.record[46] = float64(dye_5_p) / float64(total_cells)
	e.record[47] = float64(dye_6_p) / float64(total_cells)
	e.record[48] = float64(dye_7_p) / float64(total_cells)
	e.record[49] = float64(dye_8_p) / float64(total_cells)
	e.record[50] = float64(dye_1_p_n) / float64(total_cells)
	e.record[51] = float64(dye_2_p_n) / float64(total_cells)
	e.record[52] = float64(dye_3_p_n) / float64(total_cells)
	e.record[53] = float64(dye_4_p_n) / float64(total_cells)
	e.record[54] = float64(dye_5_p_n) / float64(total_cells)
	e.record[55] = float64(dye_6_p_n) / float64(total_cells)
	e.record[56] = float64(dye_7_p_n) / float64(total_cells)
	e.record[57] = float64(dye_8_p_n) / float64(total_cells)
	e.record[58] = float64(dye_1_p_c) / float64(total_cells)
	e.record[59] = float64(dye_2_p_c) / float64(total_cells)
	e.record[60] = float64(dye_3_p_c) / float64(total_cells)
	e.record[61] = float64(dye_4_p_c) / float64(total_cells)
	e.record[62] = float64(dye_5_p_c) / float64(total_cells)
	e.record[63] = float64(dye_6_p_c) / float64(total_cells)
	e.record[64] = float64(dye_7_p_c) / float64(total_cells)
	e.record[65] = float64(dye_8_p_c) / float64(total_cells)
	e.record[66] = float64(dye_1_p_m) / float64(total_cells)
	e.record[67] = float64(dye_2_p_m) / float64(total_cells)
	e.record[68] = float64(dye_3_p_m) / float64(total_cells)
	e.record[69] = float64(dye_4_p_m) / float64(total_cells)
	e.record[70] = float64(dye_5_p_m) / float64(total_cells)
	e.record[71] = float64(dye_6_p_m) / float64(total_cells)
	e.record[72] = float64(dye_7_p_m) / float64(total_cells)
	e.record[73] = float64(dye_8_p_m) / float64(total_cells)
	e.record[74] = e.float50(50, 0)
	e.record[75] = e.float50(50, 0)
	e.record[76] = e.float50(50, 0)
	e.record[77] = e.float50(50, 0)
	e.record[78] = e.float50(50, 0)
	e.record[79] = e.float50(50, 0)
	e.record[80] = e.float50(50, 0)
	e.record[81] = e.float50(50, 0)
	e.record[82] = e.float50(50, 0)
	e.record[83] = e.float50(50, 0)
	e.record[84] = e.float50(50, 0)
	e.record[85] = e.float50(50, 0)
	e.record[86] = e.float50(50, 0)
	e.record[87] = e.float50(50, 0)
	e.record[88] = e.float50(50, 0)
	e.record[89] = e.float50(50, 0)
	e.record[90] = e.float50(50, 0)
	e.record[91] = e.float50(50, 0)
	e.record[92] = e.float50(50, 0)
	e.record[93] = e.float50(50, 0)
	e.record[94] = e.float50(50, 0)
	e.record[95] = e.float50(50, 0)
	e.record[96] = e.float50(50, 0)
	e.record[97] = e.float50(50, 0)
	e.record[98] = e.floatRand(500, 5)
	e.record[99] = e.floatRand(500, 5)
	e.record[100] = e.floatRand(500, 5)
	e.record[101] = e.generateRandomInt(1000000000, 10000000)
	e.record[102] = e.generateRandomInt(5, 1)
	e.record[103] = e.Z.R.Intn(2) == 0
	e.record[104] = classList[e.classListZipf.Uint64()]
	e.record[105] = e.Z.R.Intn(2) == 0
	e.record[106] = outputType[e.outputTypeZipf.Uint64()]
	e.record[107] = dye[e.dyeZipf.Uint64()]
	e.record[108] = e.Z.R.Intn(2) == 0
	e.record[109] = e.floatRand(1, 0)
	e.record[110] = e.floatRand(1, 0)
	e.record[111] = e.floatRand(1, 0)
	e.record[112] = e.floatRand(1, 0)
	e.record[113] = mask[e.maskZipf.Uint64()]
	e.record[114] = mask[e.maskZipf.Uint64()]
	e.record[115] = mask[e.maskZipf.Uint64()]
	e.record[116] = dye[e.dyeZipf.Uint64()]
	e.record[117] = e.Z.R.Intn(2) == 0
	e.record[118] = e.floatRand(1, 0)
	e.record[119] = e.floatRand(1, 0)
	e.record[120] = e.floatRand(1, 0)
	e.record[121] = e.floatRand(1, 0)
	e.record[122] = mask[e.maskZipf.Uint64()]
	e.record[123] = mask[e.maskZipf.Uint64()]
	e.record[124] = mask[e.maskZipf.Uint64()]
	e.record[125] = dye[e.dyeZipf.Uint64()]
	e.record[126] = e.Z.R.Intn(2) == 0
	e.record[127] = e.floatRand(1, 0)
	e.record[128] = e.floatRand(1, 0)
	e.record[129] = e.floatRand(1, 0)
	e.record[130] = e.floatRand(1, 0)
	e.record[131] = mask[e.maskZipf.Uint64()]
	e.record[132] = mask[e.maskZipf.Uint64()]
	e.record[133] = mask[e.maskZipf.Uint64()]
	e.record[134] = dye[e.dyeZipf.Uint64()]
	e.record[135] = e.Z.R.Intn(2) == 0
	e.record[136] = e.floatRand(1, 0)
	e.record[137] = e.floatRand(1, 0)
	e.record[138] = e.floatRand(1, 0)
	e.record[139] = e.floatRand(1, 0)
	e.record[140] = mask[e.maskZipf.Uint64()]
	e.record[141] = mask[e.maskZipf.Uint64()]
	e.record[142] = mask[e.maskZipf.Uint64()]
	e.record[143] = dye[e.dyeZipf.Uint64()]
	e.record[144] = e.Z.R.Intn(2) == 0
	e.record[145] = e.floatRand(1, 0)
	e.record[146] = e.floatRand(1, 0)
	e.record[147] = e.floatRand(1, 0)
	e.record[148] = e.floatRand(1, 0)
	e.record[149] = mask[e.maskZipf.Uint64()]
	e.record[150] = mask[e.maskZipf.Uint64()]
	e.record[151] = mask[e.maskZipf.Uint64()]
	e.record[152] = dye[e.dyeZipf.Uint64()]
	e.record[153] = e.Z.R.Intn(2) == 0
	e.record[154] = e.floatRand(1, 0)
	e.record[155] = e.floatRand(1, 0)
	e.record[156] = e.floatRand(1, 0)
	e.record[157] = e.floatRand(1, 0)
	e.record[158] = mask[e.maskZipf.Uint64()]
	e.record[159] = mask[e.maskZipf.Uint64()]
	e.record[160] = mask[e.maskZipf.Uint64()]
	e.record[161] = dye[e.dyeZipf.Uint64()]
	e.record[162] = e.Z.R.Intn(2) == 0
	e.record[163] = e.floatRand(1, 0)
	e.record[164] = e.floatRand(1, 0)
	e.record[165] = e.floatRand(1, 0)
	e.record[166] = e.floatRand(1, 0)
	e.record[167] = mask[e.maskZipf.Uint64()]
	e.record[168] = mask[e.maskZipf.Uint64()]
	e.record[169] = mask[e.maskZipf.Uint64()]
	e.record[170] = dye[e.dyeZipf.Uint64()]
	e.record[171] = e.Z.R.Intn(2) == 0
	e.record[172] = e.floatRand(1, 0)
	e.record[173] = e.floatRand(1, 0)
	e.record[174] = e.floatRand(1, 0)
	e.record[175] = e.floatRand(1, 0)
	e.record[176] = mask[e.maskZipf.Uint64()]
	e.record[177] = mask[e.maskZipf.Uint64()]
	e.record[178] = mask[e.maskZipf.Uint64()]
	e.record[179] = e.floatRand(1, 0)
	e.record[180] = e.floatRand(1, 0)
	e.record[181] = e.floatRand(1, 0)
	e.record[182] = e.floatRand(1, 0)
	e.record[183] = e.floatRand(50, 0)
	e.record[184] = e.floatRand(1000, 100)
	e.record[185] = e.floatRand(0.5, 0)
	e.record[186] = e.floatRand(100, 1)
	e.record[187] = e.floatRand(0.5, 0)
	e.record[188] = e.floatRand(10, 0)
	e.record[189] = e.floatRand(1000, 15)
	e.record[190] = outputImage[e.outputImageZipf.Uint64()]
	e.record[191] = e.generateRandomInt(1, 0)
	e.record[192] = e.floatRand(50, 0)
	e.record[193] = e.floatRand(1000, 100)
	e.record[194] = e.floatRand(50, 0)
	e.record[195] = e.floatRand(1000, 100)
	e.record[196] = e.generateRandomInt(subjectID, 0)
	e.record[197] = species[e.speciesZipf.Uint64()]
	e.record[198] = sex[e.sexZipf.Uint64()]
	e.record[199] = startTime.Add(time.Duration(e.Z.R.Int63n(int64(timeSpan))))
	e.record[200] = e.generateRandomInt(caseID, 0)
	e.record[201] = e.generateRandomInt(95, 0)
	e.record[202] = e.generateRandomInt(200, 1)
	set := e.generateRandomInt(10, 0)
	procedureIDs := make([]uint64, 0, set)
	procedureNames := make([]string, 0, set)
	procedureDesc := make([]string, 0, set)
	for v := 0; v < set; v++ {
		procedureIDs = append(procedureIDs, uint64(e.generateRandomInt(10000, 0)))
		procedureNames = append(procedureNames, "P"+strconv.Itoa(v))
		procedureDesc = append(procedureDesc, procedures_desc[e.procedures_descZipf.Uint64()])
	}
	e.record[203] = procedureIDs
	e.record[204] = procedureNames
	e.record[205] = procedureDesc
	e.record[206] = e.generateRandomInt(10000, 1)
	e.record[207] = e.generateRandomInt(20000, 1)
	e.record[208] = comments[e.commentsZipf.Uint64()]
	e.record[209] = e.generateRandomInt(10000, 1)
	e.record[210] = disease[e.diseaseZipf.Uint64()]
	e.record[211] = etiology[e.etiologyZipf.Uint64()]
	e.record[212] = morphology[e.morphologyZipf.Uint64()]
	e.record[213] = e.generateRandomInt(10000, 1)
	e.record[214] = e.generateRandomInt(specimenID, 0)
	e.record[215] = tissuetype[e.tissuetypeZipf.Uint64()]
	e.record[216] = e.generateRandomInt(500, 1)
	e.record[217] = locationTissue[e.locationTissueZipf.Uint64()]
	e.cur++

	return e.record, nil
}

// Schema implements idk.Source.
func (e *MerckSource) Schema() []idk.Field {
	return e.schema
}

func (e *MerckSource) Seed(seed int64) {
	e.rand.Seed(seed)
}

var _ Seedable = (*MerckSource)(nil)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// returns a string with random characters from a predefined charset of a specified length
func (e *MerckSource) StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[e.rand.Intn(len(charset))]
	}
	return string(b)
}

//returns a string with random English alphabets of the specified length
func (e *MerckSource) String(length int) string {
	return e.StringWithCharset(length, charset)
}

//returns a random int between a range with a 50% chance of returning a zero.
func (e *MerckSource) generate50(max int, min int) int {
	num := e.rand.Intn(100)
	numbertoReturn := e.rand.Intn(max-min) + min
	switch {
	case num < 50:
		return 0
	default:
		return numbertoReturn
	}
}

//returns a random int between a range with a 10% chance of returning a zero.
func (e *MerckSource) generate10(max int, min int) int {
	num := e.rand.Intn(100)
	numbertoReturn := e.rand.Intn(max-min) + min
	switch {
	case num < 10:
		return 0
	default:
		return numbertoReturn
	}
}

//returns a random float between a range with a 50% chance of returning a zero.
func (e *MerckSource) float50(max float64, min float64) float64 {
	numbertoReturn := min + e.rand.Float64()*(max-min)
	num := e.rand.Intn(100)
	switch {
	case num < 50:
		return 0
	default:
		return numbertoReturn
	}
}

//returns a random float between a range
func (e *MerckSource) floatRand(max float64, min float64) float64 {
	return min + e.rand.Float64()*(max-min)
}

//returns a random int between a range
func (e *MerckSource) generateRandomInt(max int, min int) int {
	return e.rand.Intn(max-min) + min
}

func (e *MerckSource) Close() error {
	return nil
}
