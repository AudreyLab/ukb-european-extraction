# UK Biobank European Participants Extraction

This repository contains Python scripts to extract demographic data from UK Biobank and filter for European ancestry participants, optimized for high-performance computing environments like Compute Canada.

## Overview

The UK Biobank contains data from over 500,000 participants with diverse ethnic backgrounds. This toolkit provides memory-efficient extraction of demographic fields and filtering for European ancestry participants based on both self-reported ethnicity and genetic ancestry.

## Features

- **Memory-efficient processing**: Handles large UKB files (500K+ participants) with chunked reading
- **Selective column extraction**: Extracts only relevant demographic fields from 2,000+ column datasets
- **European ancestry filtering**: Uses standard UKB ethnicity codes for participant selection
- **HPC optimized**: Works on Compute Canada clusters (Narval, Cedar, etc.) without additional dependencies
- **Comprehensive reporting**: Provides detailed statistics and verification outputs

## Requirements

- Python 3.6+
- pandas
- numpy
- Standard Python libraries (os, gc, sys)

No additional installations required on Compute Canada systems.

## Input Data Format

Expected input: UK Biobank tab-separated file (.tab) containing:
- Participant IDs (f.eid)
- Demographic fields (f.31.*, f.21000.*, etc.)
- Genetic ancestry data (f.22006.*, f.22009.*)

## Scripts

### 1. `extract_ukb_demographics.py`

Extracts demographic data from the full UK Biobank dataset.

**Extracted fields:**
- `f.31.*` → Sex
- `f.21000.*` → Ethnic background (self-reported)
- `f.21003.*` → Age at visit
- `f.22001.*` → Genetic sex
- `f.22006.*` → Genetic ethnic grouping
- `f.22009.*` → Principal components (PC1-PC40)
- `f.22010.*` → Genotype analysis exclusions
- `f.22018.*` → Relatedness exclusions

**Usage:**
```bash
python extract_ukb_demographics.py
```

**Input:** `ukb8045.r.tab` (or modify filename in script)
**Output:** `demographic_data.tsv`

### 2. `filter_european_participants.py`

Filters demographic data to retain only European ancestry participants.

**European ancestry criteria:**
- **Self-reported ethnicity (field 21000):**
  - 1001 = British
  - 1002 = Irish
  - 1003 = Any other white background
- **Genetic ancestry (field 22006):**
  - 1 = Caucasian

**Usage:**
```bash
python filter_european_participants.py
```

**Input:** `demographic_data.tsv`
**Output:** 
- `european_participants.tsv` - Complete demographic data for Europeans
- `european_participant_ids.txt` - List of European participant IDs only

## Memory Optimization

The scripts use several strategies for large dataset processing:

- **Chunked reading**: Processes data in 3,000-row chunks
- **Selective loading**: Only loads required columns
- **Memory monitoring**: Tracks RAM usage throughout processing
- **Garbage collection**: Explicit memory cleanup between operations

## Usage on Compute Canada

### Basic execution:
```bash
python extract_ukb_demographics.py
python filter_european_participants.py
```

### With SLURM resource allocation:
```bash
srun --time=1:00:00 --mem=8G python extract_ukb_demographics.py
srun --time=0:30:00 --mem=4G python filter_european_participants.py
```

### Interactive session:
```bash
salloc --time=2:00:00 --mem=16G
python extract_ukb_demographics.py
python filter_european_participants.py
```

## Output Files

### `demographic_data.tsv`
Complete demographic data with renamed columns:
- ID, Sex, Ethnic_backgr_inst1-3, Age_at_Visit_inst1-3
- Genetic_sex, Gen_ethnic_grp, PC1-PC15 (or PC1-PC40)
- Geno_analys_exclns, Relat_exclns

### `european_participants.tsv`
Same structure as above, filtered for European participants only.

### `european_participant_ids.txt`
Simple list of participant IDs for European participants (one ID per line).

## Quality Control

The scripts provide comprehensive reporting:
- Initial dataset statistics
- Column identification and extraction summary
- Memory usage monitoring
- Pre/post-filtering ethnic distribution
- Final participant counts and percentages

## Example Output

```
=== Extraction de données démographiques UK Biobank sur Narval ===
Données initiales : 502641 participants, 2466 colonnes
Colonnes à extraire : 27
Données extraites : 502641 lignes, 27 colonnes

=== Filtrage des participants européens ===
Participants européens identifiés : 409,692
Pourcentage : 81.5%
```

## Troubleshooting

### Memory Issues
- Reduce `chunksize` parameter (default: 3000)
- Request more memory: `srun --mem=16G`
- Check available memory: `free -h`

### File Not Found
- Verify input file name in script
- Check file permissions: `ls -la *.tab`

### Column Mismatch
- Examine file structure: `head -1 your_file.tab`
- Verify field codes match your UKB data dictionary

## Customization

### Modify extracted fields:
Edit the `fields`, `array_length`, `instances`, and `labels` lists in `extract_ukb_demographics.py`

### Change European ancestry criteria:
Modify `european_codes_ethnic` and `european_codes_genetic` in `filter_european_participants.py`

### Adjust memory usage:
Change `chunksize` parameter in extraction functions

## Performance Metrics

**Typical processing times (Compute Canada):**
- Demographic extraction: 5-10 minutes (8GB RAM)
- European filtering: 1-2 minutes (4GB RAM)

**Memory usage:**
- Peak: ~200MB for 500K participants
- Output file: ~40MB demographic data, ~30MB European subset

## License

This code is provided as-is for research purposes. Please cite appropriately and follow UK Biobank data usage guidelines.

## Contributing

Feel free to submit issues or pull requests to improve memory efficiency or add additional demographic filters.

## References

- UK Biobank Data Dictionary: https://biobank.ndph.ox.ac.uk/showcase/
- Field 21000 (Ethnic background): https://biobank.ndph.ox.ac.uk/showcase/field.cgi?id=21000
- Field 22006 (Genetic ethnic grouping): https://biobank.ndph.ox.ac.uk/showcase/field.cgi?id=22006
