# The AGSD Taxonomy Update Pipeline

This python script updates and fills in the taxonomic data of the Animal Genome Size Database (AGSD) by crross-referencing records against the ChecklistBank API and the Global Namer Verifier (GNV).

## General workflow
1. Extracting taxonomic data from the AGSD a .sql genome entries file
2. Lower-order matching of names in AGSD to a ChecklistBank dataset and veriication with GNV as required by querying their respective APIs
3. Family name matching for records that did not previously match (Again, using ChecklistBank and GNV)
4. Merging of matched taxonomic data with records in the AGSD
5. File export

<img width="4638" height="5550" alt="Blank diagram" src="https://github.com/user-attachments/assets/ee67b52e-8477-4274-9a83-0837f27ab359" />

## Inputs required:
1. **The AGSD genome entries .sql file**, saved to the same directory as the script - or with a path given
2. **The key identifier of the ChecklistBank checkilist you wish to cross-referernce against**. For the purpose of this analysis, the 2025 annual release Catalogue of Life checklist (CoL25) is used - key identifer **310463**. For other available datasets and their associated identification keys, please see the ChecklistBank site (https://www.checklistbank.org/dataset).

## Outputs:
**.CSV output files: **
1. The full updated AGSD data 
2. Low-order matches and associated metadata 
3. Family-level matches and associated metadata
4. All unmatched records 
5. Match errors

**.txt files:**
1. Updated tax. names log
2. Filled tax names log
3. Tax. reclassifation log
4. High tax. update log

## Requirements:
Python 3.x 
