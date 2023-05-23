## Mappers folder documentation

This folder contains files mapping codes from one ontology to another one. All these
mapping files have been curated by BenevolentAI scientists. Notice that these mappings
should be considered unidirectional; and the directionality can be inferred from the file
name.
The `parquet` files in this folder are the following:

### Ontology mappings
- **icd10_to_phecodes**, contains the mappings from ICD10 codes to Phecodes.
- **icd10_to_mondo**, contains the mappings from ICD10 codes to MONDO codes. 
- **mondo_to_icd10**, contains the mappings from MONDO codes to ICD10 codes. 
- **readcode_to_icd10**, contains the mappings from Readcodes to ICD10 codes.
- **medication_to_atc**, contains the mappings from UKBB medications to ATC and CID codes.
- **atc_chil_to_parent**, contains the mapping from child ATC codes to parent ATC codes.
- **ac_to_opcs4**, contains the mapping from the assessment centre operations to OPCS4 parent operation codes. 
It only includes AC codes reported in at least 100 participants. 
- **opcs3_to_opcs4**, contains the mappings from OPCS3 operation codes to OPCS4 parent operation codes. 
It only includes OPCS3 codes reported in at least 100 participants. 