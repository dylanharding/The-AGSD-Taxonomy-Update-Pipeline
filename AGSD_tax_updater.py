import requests
import re
import csv
import time
import os
from requests.auth import HTTPBasicAuth
from io import StringIO

# Request for CheckilistBank user key
def fetch_user_key(username, password):
    url = "https://api.checklistbank.org/user/me"
    r = requests.get(url, auth=HTTPBasicAuth(username, password))
    data = r.json()
    user_key = data["key"]
    return(user_key)

# Extracting data from the AGSD .sql file
def AGSD_data_extract(AGSD_sql_file):
    print("Extracting AGSD tax data...")
    print("-"*15)
    time.sleep(1)
 
    AGSD_records = []
    column_names = []
    
    with open(AGSD_sql_file, "r") as file:
        for line in file:
            stripped = line.strip()

            # Using the INSERT INTO line to find keys
            if not column_names and stripped.lower().startswith("insert into"):
                columns = re.search(r"\((.*?)\)", stripped)
                if columns:
                    column_names = [line.strip().strip('`"') for line in columns.group(1).split(',')]

                    # Changing some key names for consistency with data downstream
                    column_renames = {
                            "sub_phylum": "subphylum",
                            "super_class": "superclass",
                            "sub_class": "subclass",
                            "infra_class": "infraclass",
                            "super_order": "superorder",
                            "order_name": "order",
                            "sub_order": "suborder",
                            "infra_order": "infraorder",
                            "sub_family": "subfamily",
                            "species_alt": "name_in_reference"
                        }
                    
                column_names = [column_renames.get(name, name) for name in column_names]


            # Cleaning and appending values from records
            elif re.match(r'^\(\d+', stripped):
                stripped = stripped[1:-2]
                csv_reader = csv.reader(StringIO(stripped), skipinitialspace = True, quotechar ="'")
                entry_row = next(csv_reader)

                cleaned_data = []

                for data in entry_row:
                    data = data.replace('\xa0', ' ').strip() # Removes hidden character spaces that ccan be found in SQL files

                    # Converting all "NULL" values or empty strings to None type
                    if data.upper() == "NULL" or data in ("''", "", "' '", '" "', " "): 
                        cleaned_data.append(None)
                    elif data.startswith("'") and data.endswith("'"):
                        data = data[1:-1].strip()
                        cleaned_data.append(data)
                    else:
                        cleaned_data.append(data)

                if len(cleaned_data) < len(column_names):
                    continue

                # Zipping record data with key (column) names
                row_data = dict(zip(column_names, cleaned_data))

                subspecies_value = row_data.get("subspecies")
                species_value = row_data.get("species")

                # Assigning query ranks and namees. "ssp." and "sp." are removed from query names, 
                # however result in a querying ranks of species and genus, respectively
                if subspecies_value:
                    if "ssp." in subspecies_value:
                        raw_name = subspecies_value
                        query_name = subspecies_value.split("ssp.")[0].strip()
                        query_rank = "species"
                    else:
                        raw_name = subspecies_value
                        query_name = subspecies_value
                        query_rank = "subspecies"
                else:
                    if "ssp." in species_value:
                        raw_name = species_value
                        query_name = species_value.split("ssp.")[0].strip()
                        query_rank = "species"
                    elif "sp." in species_value:
                        raw_name = species_value
                        query_name = species_value.split("sp.")[0].strip()
                        query_rank = "genus"
                    else:
                        raw_name = species_value
                        query_name = species_value
                        query_rank = "species"

                if row_data["kingdom"] is None:
                    row_data["kingdom"] = "Animalia"

                AGSD_records.append({
                    **row_data,
                    "query_name": query_name,
                    "query_rank": query_rank,
                    "raw_name": raw_name
                })

        return(AGSD_records)

# Matching names using the CheckListBank and Global Names Verifier (GNV) APIs, and CoL25 as the reference dataset
def tax_namematch(dataset, AGSD_records, list_name):

    record_tot = len(AGSD_records)

    all_matches = []
    all_unmatches = []
    all_match_issues = []
    all_errors = []
    tax_classification_list = {}
    match_tax_cache = {}
    unmatch_tax_cache = {}

    print(f"Checking {list_name} names against dataset {dataset}...")
    print(f"Start time {time.strftime('%H:%M:%S')}")

    total_count = 0
    GNV_count = 0  
    for record in AGSD_records:
        
        id = record.get("id")
        raw_name = record.get("raw_name")
        query_name = record.get("query_name")
        query_rank = record.get("query_rank")

        # Result caching temporarily deactived
        '''cache_key = f"{query_name}_{query_rank}"

        if cache_key in match_tax_cache:
            results = match_tax_cache[cache_key].copy()
            results["id"] = id
            all_matches.append(results)
            if total_count % 100 == 0 and total_count != 0:
                print(f"{total_count}/{record_tot} records processed")
            total_count += 1
            cache_lookup_count += 1
            continue

        if cache_key in unmatch_tax_cache:
            results = unmatch_tax_cache[cache_key].copy()
            results["id"] = id
            all_unmatches.append(results)
            if total_count % 100 == 0 and total_count != 0:
                print(f"{total_count}/{record_tot} records processed")
            total_count += 1
            cache_lookup_count += 1
            continue'''

        formatted_name = query_name.replace(" ", "%20")
        url = f"https://api.checklistbank.org/dataset/{dataset}/match/nameusage?scientificName={formatted_name}&rank={query_rank}"
      
        # Querying ChecklistBank
        try:
            r = requests.get(url, auth=HTTPBasicAuth(username, password))
            r.raise_for_status()
            data = r.json()

            # Re-querying with rank changed to "subspecies" if ChecklistBank has flagged a match as such
            if data and len(data.get("issues")) > 0:
                if 'subspecies assigned' in data.get("issues").get("issues"):
                    query_rank = "subspecies_adjusted"
                    url = f"https://api.checklistbank.org/dataset/{dataset}/match/nameusage?scientificName={formatted_name}&rank=subspecies"

                    try:
                        r = requests.get(url, auth=HTTPBasicAuth(username, password))
                        r.raise_for_status()
                        data = r.json()

                    except Exception as e:
                        print(f"Error processing name {raw_name}: {e}")
                        all_errors.append({
                        "raw_name": raw_name,
                        "query_name": query_name,
                        "query_rank": query_rank,
                        "error": e
                        })

            # Adding all successfull match data (metadata + taxonomic) to a results list
            if data and data.get("match") == True and not data.get("issues"):
                results = {
                "id": id,
                "raw_name": raw_name,
                "query_name": query_name,
                "query_rank": query_rank,
                "match_id": data.get("usage", {}).get("id"),
                "match_type": data.get("usage", {}).get("namesIndexMatchType"),
                "status": data.get("usage", {}).get("status"),
                "match_rank": data.get("usage", {}).get("rank"),
                "name_authorship": data.get("usage", {}).get("authorship"),
                "nidx": data.get("usage", {}).get("namesIndexId"),
                "issues": data.get("issues")
                }

                classification = data.get("usage", {}).get("classification", [])
                for group in classification:
                    tax_rank = group["rank"]
                    tax_name = group["name"]
                    tax_id = group["id"]

                    if tax_name not in tax_classification_list:
                        tax_classification_list[tax_name] = []
                    if tax_rank not in tax_classification_list[tax_name]:
                        tax_classification_list[tax_name].append(tax_rank)

                    results[tax_rank] = tax_name
                    results[f"{tax_rank}_COL_code"] = tax_id

                    if tax_rank == "kingdom":
                        break
                
                all_matches.append(results)
                match_tax_cache[f"{query_name}_{query_rank}"] = results
                
            # Querying GNV using unmatched names
            else:
                GNV_match_name, GNV_edit_distance, call_error = global_names_verifier(raw_name, query_rank, query_name, formatted_name)
              
                if len(call_error) > 0:
                    all_errors.append(call_error)

                # If GNV finds no match, add to unmatched list
                elif GNV_match_name is None and GNV_edit_distance is None:
                    results = {**record, "issues": data.get("issues")}
                    all_unmatches.append(results)
                    unmatch_tax_cache[f"{query_name}_{query_rank}"] = results

                # If GNV does find a match, re-query the ChecklistBank API using that matched name
                else:
                    formatted_name = GNV_match_name.replace(" ", "%20")
                    url = f"https://api.checklistbank.org/dataset/{dataset}/match/nameusage?scientificName={formatted_name}&rank={query_rank}"

                    try:
                        r = requests.get(url, auth=HTTPBasicAuth(username, password))
                        r.raise_for_status()
                        data = r.json()

                        if data and data.get("match") == True:
                            results = {
                            "id": id,
                            "raw_name": raw_name,
                            "query_name": query_name,
                            "query_rank": query_rank,
                            "match_id": data.get("usage", {}).get("id"),
                            "match_type": data.get("usage", {}).get("namesIndexMatchType"),
                            "status": data.get("usage", {}).get("status"),
                            "match_rank": data.get("usage", {}).get("rank"),
                            "scientific_name": data.get("usage", {}).get("name"),
                            "name_authorship": data.get("usage", {}).get("authorship"),
                            "nidx": data.get("usage", {}).get("namesIndexId"),
                            "issues": data.get("issues"),
                            "GNV_required": "True",
                            "GNV_edit_distance": GNV_edit_distance
                            }

                            classification = data.get("usage", {}).get("classification", [])
                            for group in classification:
                                tax_rank = group["rank"]
                                tax_name = group["name"]
                                tax_id = group["id"]

                                if tax_name not in tax_classification_list:
                                    tax_classification_list[tax_name] = []
                                if tax_rank not in tax_classification_list[tax_name]:
                                    tax_classification_list[tax_name].append(tax_rank)

                                results[tax_rank] = tax_name
                                results[f"{tax_rank}_COL_code"] = tax_id

                                if tax_rank == "kingdom":
                                    break
                            
                            if len(results["issues"]) > 0:
                                all_match_issues.append(results)
                            else:
                                all_matches.append(results)
                                match_tax_cache[f"{query_name}_{query_rank}"] = results
                                GNV_count += 1

                        else:
                            results = {**record, "issues": data.get("issues")}
                            all_unmatches.append(results)
                            unmatch_tax_cache[f"{query_name}_{query_rank}"] = results

                    except Exception as e:
                        print(f"Error processing GNV corrected name {raw_name}: {e}")
                        all_errors.append({
                            "raw_name": raw_name,
                            "query_name": query_name,
                            "query_rank": query_rank,
                            "error": e
                            })
                        
        except Exception as e:
                    print(f"Error processing name {raw_name}: {e}")
                    all_errors.append({
                        "raw_name": raw_name,
                        "query_name": query_name,
                        "query_rank": query_rank,
                        "error": e
                        })

        # Small time sleep to prevent overwhelming API calls
        time.sleep(0.1)
        if total_count % 100 == 0 and total_count != 0:
            print(f"{total_count}/{record_tot} records processed")
        total_count += 1

    print(f"Finished {time.strftime('%H:%M:%S')}")
    print(f"{len(all_matches)} records matched ({(len(all_matches)/total_count)*100}%)")
    print(f"{len(all_unmatches)} records unmatched ({(len(all_unmatches)/total_count)*100}%)")
    print(f"{len(all_match_issues)} record match issues ({(len(all_match_issues)/total_count)*100}%)")
    print(f"{len(all_errors)} record matches failed due to error ({(len(all_errors)/total_count)*100}%)")
    print(f"{GNV_count} ({(GNV_count/record_tot)*100}%) names corrected with GNverifier")

    print("-"*15)
    
    return(all_matches, all_unmatches, all_match_issues, all_errors, tax_classification_list)

# Function used for GNV API calls
def global_names_verifier(raw_name, query_rank, query_name, formatted_name):
   
        url = f"https://verifier.globalnames.org/api/v1/verifications/{formatted_name}?data_sources=1&all_matches=false&capitalize=True&species_group=false&fuzzy_uninomial=false&stats=false&main_taxon_threshold=0.5"
        call_error = []

        # Querying GNV
        try:
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()

        except Exception as e:
            print(f"Error verifying name {raw_name} with GNverifier: {e}")
            call_error.append({
                "raw_name": raw_name,
                "query_name": query_name,
                "query_rank": query_rank,
                "error": e
                })
            
        # Taking only returned matches where the query rank is equal to match rank.
        if data and data.get("names")[0].get("matchType") != "NoMatch":
            GNV_top_match = data.get("names")[0].get("bestResult")
            GNV_tax_match = GNV_top_match.get("classificationRanks")
            if GNV_tax_match.endswith(f"{query_rank}"):
                GNV_match_name = GNV_top_match.get("matchedCanonicalFull")
                GNV_edit_distance = GNV_top_match.get("editDistance")
            else:
                GNV_match_name = None
                GNV_edit_distance = None
        else:
            GNV_match_name = None
            GNV_edit_distance = None

        return(GNV_match_name, GNV_edit_distance, call_error)

# Small fucntion separate ambiguous from non-ambiguous matches in data
def ambiguous_match_extract(match_list):
    clean_matches = []
    ambiguous_matches = []

    for match in match_list:
        if match["match_type"] == "ambiguous" or match["status"] == "ambiguous synonym":
            ambiguous_matches.append(match)
        else:
            clean_matches.append(match)
    
    return (clean_matches, ambiguous_matches)


# Family namematch function to re-query ChecklistBank for previoulsy unmatched records using family name
def family_namematch(dataset, unmatches, list_name):
    print(f"Matching family names for unmatched species entries...")

    for record in unmatches:
        record['query_name'] = record['family']
        record['query_rank'] = "family"
        if record.get('raw_name'):
            del record['raw_name']
        if record.get("issues"):
            del record['issues']

    matches, unmatches, match_issues, match_errors, higher_tax_class_list = tax_namematch(dataset, unmatches, list_name)


    return(matches, unmatches, match_issues, match_errors, higher_tax_class_list)

# Appending key identifiers to records for the sources providing taxonomic information to CoL25.
def append_source_keys(dataset, matches):
    print(f"Obtaining source keys for matches...")
    print(f"Start time {time.strftime('%H:%M:%S')}")

    count = 0
    key_cache = {}

    for record in matches:
        match_id = record["match_id"]
        if match_id in key_cache:
            source_key = key_cache[match_id]
        else:
            source_key = fetch_source_keys(dataset, match_id)
            source_key = str(source_key)
            key_cache[match_id] = source_key
            time.sleep(0.1)

        record["source_key"] = source_key

        count += 1
        if count % 100 == 0:
            print(f"{count} keys retrieved")
        else:
            continue
    
    print(f"Finished {time.strftime('%H:%M:%S')}")
    print(f"Updated match list with CoL match IDs")
    print("-"*15)
    return(matches)

# The API call function that returns the ChecklistBank source key using dataset and match ID
def fetch_source_keys(dataset, match_id):
    url = f"https://api.checklistbank.org/dataset/{dataset}/nameusage/{match_id}/source"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        source_key = data.get("sourceDatasetKey", None)
        source_key = str(source_key)
        # Removing any numbers with trailing ".0" or ".00"
        source_key = re.sub(r"(\d+)\.\d+$", r"\1", source_key)

    except Exception as e:
        print(f"Error fetching source for match ID {match_id}: {e}")
        return None
    
    return(source_key)

# Appending source names to records using source key identifiers
def append_source_names(dataset, matches):
    print(f"Obtaining sources for taxonomic matches...")
    print(f"Start time {time.strftime('%H:%M:%S')}")
    
    name_cache = {}
    count = 0

    for record in matches:
        source_key = record["source_key"]
        if source_key in name_cache:
            tax_source_name = name_cache[source_key]
        else:
            tax_source_name = fetch_source_names(dataset, source_key)
            name_cache[source_key] = tax_source_name
            time.sleep(0.1)

        record["tax_source_name"] = tax_source_name
        count += 1
        if count % 100 == 0:
            print(f"{count} sources retrieved")
        else:
            continue
    
    print(f"Finished {time.strftime('%H:%M:%S')}")
    print(f"Updated matched records with source names")
    print("-"*15)
    return(matches)

# The API call function that returns the ChecklistBank source dataset using the source key
def fetch_source_names(dataset, source_key):
    url = f"https://api.checklistbank.org/dataset/{dataset}/source/{source_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("title", None)
    
    except Exception as e:
        print(f"Error fetching name with source key {source_key}: {e}")
        return None

# Data merging function that merges matched data with AGSD records
def data_merger(AGSD_records, matched_list):

    merged_data = []
    high_tax_update_log = {}
    matched_lookup = {}
    tax_update_log = {}
    tax_fill_log = {}
    tax_reclass_log = {}
    higher_tax_list = ["kingdom", "phylum", "subphylum", "superclass", "class", "subclass", "infraclass", "superorder", "order"]
    non_tax_keys  = ["id", "match_id", "match_type", "match_rank", "status", "query_name", "query_rank", "raw_name", "scientific_name",
    "source_key", "tax_source_name", "source_name", "name_authorship", "name_in_reference", "entered_by", "date_entered", "date_last_modified", "c_value", "c_value_upper", 
    "chrom_num", "chrom_num_upper", "GNV_edit_distance", "GNV_required", "method", "std_sp", "rr", "comments", "refs", "issues", "species_synonyms", 
    "subspecies_synonyms", "family_synonyms", "genus_synonyms", "tax_filled", "tax_updated", "type", "nidx", "order_alt", "common_name"]

    # Using ID as matched record key value for efficient lookup
    for match in matched_list:
        matched_lookup[match["id"]] = match

    # For each record in the AGSD data, looks up the corresonding matched record using id.
    for old_record in AGSD_records:
        tax_filled = []
        tax_updated = []
        id = old_record["id"]
        matched_record = matched_lookup.get(id)

        #### MERGING CONDITIONS ####

        ## NO MATCH
        if matched_record == None: # Uses original record if no match found
            merged_data.append(old_record)
            continue
        
        ## AMBIGUOUS MATCHES 
        elif matched_record["match_type"] == "ambiguous" or matched_record["status"] == "ambiguous synonym": # Uses original record if match type is ambiguous
            merged_data.append(old_record)
            continue

        ## FAR-OFF MATCHES, use orininal record if match is not in animal kingdom.
        elif matched_record["kingdom"] != "Animalia":
            merged_data.append(old_record)
            continue

        ## SPECIES-LEVEL MATCHES
        elif matched_record["query_rank"] == matched_record['match_rank'] == 'species':
            matched_record["species_COL_code"] = matched_record['match_id']
            combined_record = old_record.copy()
            
            # Merging straight-forward accepted species match
            if matched_record["status"] in ("accepted","provisionally accepted"):
                for key, new_value in matched_record.items():

                    # If not a tax name, simply add data from matched record
                    if (key in non_tax_keys) or ("_COL_code" in key):
                        combined_record[key] = new_value
                        continue

                    # Keep track of reclassifications, where a name is added that already exists, but to a different tax rank, and remove name from old rank
                    for old_key, old_value in combined_record.items():
                        if (old_value == new_value) and (old_key != key) and ("_COL_code" not in old_key) and (old_key not in non_tax_keys):
                            combined_record[old_key] = None

                            if id not in tax_reclass_log:
                                tax_reclass_log[id] = []
                            tax_reclass_log[id].append(f"{old_value} reclassified from {old_key} to {key}")
                    
                    old_value = combined_record.get(key)
                    if new_value != old_value: # Tax updates - where a different value already existed for the that rank in old dataset
                        if old_value != None: 
                            if id not in tax_update_log:
                                tax_update_log[id] = []
                            tax_updated.append(key)
                            tax_update_log[id].append(f"{key} changed from '{old_value}' to '{new_value}'")
                            
                            if key in higher_tax_list: # Keeping track of high tax changes in high tax change log
                                if id not in high_tax_update_log:
                                    high_tax_update_log[id] = []
                                high_tax_update_log[id].append(f"WARNING: {key} changed from '{old_value}' to '{new_value}'")

                        if old_value == None: # Tax fills - where no value existed for that rank in the old dataset
                            if id not in tax_fill_log:
                                tax_fill_log[id] = []
                            tax_filled.append(key)
                            tax_fill_log[id].append(f"{key} classification '{new_value}' added")
                        combined_record[key] = new_value

                combined_record["date_last_modified"] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                combined_record["tax_filled"] = tax_filled
                combined_record["tax_updated"] = tax_updated
                merged_data.append(combined_record)

            # Merging reecords for returned species synonyms
            elif matched_record['status'] == "synonym":

                # Removing subspecies data that is sometimes returned by species querying to ChecklistBank, however not considered reliable here.
                if matched_record.get('subspecies') != None:
                    matched_record['subspecies'] = None
                    matched_record['subspecies_COL_code'] = None
                else:
                    combined_record['species_synonyms'] = combined_record["species"] # Moving old name to "synonyms" column
                
                for key, new_value in matched_record.items():

                    # If not a tax name, simply add data from matched record
                    if (key in non_tax_keys) or ("_COL_code" in key):
                        combined_record[key] = new_value
                        continue
                    
                    # Keep track of reclassifications
                    for old_key, old_value in combined_record.items():
                        if (old_value == new_value) and (old_key != key) and ("_COL_code" not in old_key) and (old_key not in non_tax_keys):
                            combined_record[old_key] = None

                            if id not in tax_reclass_log:
                                tax_reclass_log[id] = []
                            tax_reclass_log[id].append(f"{old_value} reclassified from {old_key} to {key}")
                    
                    old_value = combined_record.get(key)
                    if new_value != old_value: 

                        # Tax updates
                        if old_value != None:
                            if id not in tax_update_log:
                                tax_update_log[id] = []
                            if key in higher_tax_list: # Keep track of high tax changes
                                if id not in high_tax_update_log:
                                    high_tax_update_log[id] = []
                                high_tax_update_log[id].append(f"WARNING: {key} changed from '{old_value}' to '{new_value}'")

                            if key == "species": # Dealing with synonymym swapping of species names that have extra information in name 
                                bracket_designation = r"\([0-9]+[A-Za-z]*\)" # Regex pattern for bracketed numbers+letters in names (eg. Bufo viridis (4n)) for synonym swapping later
                                ssp_designation = r"\bssp\..*" # Regex pattern for "ssp.*"
                                bracket_match = re.search(bracket_designation, old_value)
                                ssp_match = re.search(ssp_designation, matched_record['raw_name'])
                                if bracket_match:
                                    new_value = f"{new_value} {bracket_match.group(0)}"
                                if ssp_match:
                                    new_value = f"{new_value} {ssp_match.group(0)}"

                            tax_updated.append(key)
                            tax_update_log[id].append(f"{key} changed from '{old_value}' to '{new_value}'")

                        # Tax fills
                        if old_value == None:
                            if id not in tax_fill_log:
                                tax_fill_log[id] = []
                            tax_filled.append(key)
                            tax_fill_log[id].append(f"{key} classification '{new_value}' added")
                        combined_record[key] = new_value

                combined_record["date_last_modified"] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                combined_record["tax_filled"] = tax_filled
                combined_record["tax_updated"] = tax_updated
                merged_data.append(combined_record)

        ## GENUS-LEVEL MATCHES
        elif matched_record["query_rank"] == matched_record["match_rank"] == "genus":
            matched_record["genus_COL_code"] = matched_record["match_id"]
            combined_record = old_record.copy()

            # Merge for accepted genus name match
            if matched_record["status"] in ("accepted","provisionally accepted"):
                matched_record['genus'] = matched_record['query_name'] # Moves genus name to new genus column
                matched_record['species'] = matched_record['raw_name'] # Keeps original (sp.) name in species column
                
                for key, new_value in matched_record.items():

                    # If not a tax name, simply add data from matched record
                    if (key in non_tax_keys) or ("_COL_code" in key):
                        combined_record[key] = new_value
                        continue
                    
                    # Keep track of reclassifications
                    for old_key, old_value in combined_record.items():
                        if (old_value == new_value) and (old_key != key) and ("_COL_code" not in old_key) and (old_key not in non_tax_keys):
                            combined_record[old_key] = None

                            if id not in tax_reclass_log:
                                tax_reclass_log[id] = []
                            tax_reclass_log[id].append(f"{old_value} reclassified from {old_key} to {key}")
                    
                    old_value = combined_record.get(key)
                    if new_value != old_value:

                        # Tax updates
                        if old_value != None: 
                            if id not in tax_update_log:
                                tax_update_log[id] = []
                            tax_updated.append(key)
                            tax_update_log[id].append(f"{key} changed from '{old_value}' to '{new_value}'")
                            if key in higher_tax_list:
                                if id not in high_tax_update_log:
                                    high_tax_update_log[id] = []
                                high_tax_update_log[id].append(f"WARNING: {key} changed from '{old_value}' to '{new_value}'")

                        # Tax fills      
                        if old_value == None:
                            if id not in tax_fill_log:
                                tax_fill_log[id] = []
                            tax_filled.append(key)
                            tax_fill_log[id].append(f"{key} classification '{new_value}' added")
                        combined_record[key] = new_value

                combined_record["date_last_modified"] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                combined_record["tax_filled"] = tax_filled
                combined_record["tax_updated"] = tax_updated
                merged_data.append(combined_record)
            
            # Genera synonyms
            elif matched_record['status'] == "synonym":
                combined_record['genus_synonyms'] = combined_record["species"] # Moves old name to "synonyms" column
                matched_record['genus_COL_code'] = matched_record["match_id"]
                
                for key, new_value in matched_record.items():

                    # If not a tax name, simply add data from matched record
                    if (key in non_tax_keys) or ("_COL_code" in key):
                        combined_record[key] = new_value
                        continue
                    
                    # Keep track of reclassifications
                    for old_key, old_value in combined_record.items():
                        if (old_value == new_value) and (old_key != key) and ("_COL_code" not in old_key) and (old_key not in non_tax_keys):
                            combined_record[old_key] = None

                            if id not in tax_reclass_log:
                                tax_reclass_log[id] = []
                            tax_reclass_log[id].append(f"{old_value} reclassified from {old_key} to {key}")
                    
                    old_value = combined_record.get(key)
                    if new_value != old_value:

                        # Tax updates
                        if old_value != None: 
                            if id not in tax_update_log:
                                tax_update_log[id] = []
                            tax_updated.append(key)
                            tax_update_log[id].append(f"{key} changed from '{old_value}' to '{new_value}'")
                            if key in higher_tax_list:
                                if id not in high_tax_update_log:
                                    high_tax_update_log[id] = []
                                high_tax_update_log[id].append(f"WARNING: {key} changed from '{old_value}' to '{new_value}'")

                        # Tax fills      
                        if old_value == None:
                            if id not in tax_fill_log:
                                tax_fill_log[id] = []
                            tax_filled.append(key)
                            tax_fill_log[id].append(f"{key} classification '{new_value}' added")
                        combined_record[key] = new_value

                combined_record["date_last_modified"] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                combined_record["tax_filled"] = tax_filled
                combined_record["tax_updated"] = tax_updated
                merged_data.append(combined_record)

        ## SUBSPECIES-LEVEL MATCHES
        elif (matched_record["match_rank"] == "subspecies") and (matched_record["query_rank"] in ("subspecies", "subspecies_adjusted")):
            combined_record = old_record.copy()
            if matched_record["status"] in ("accepted","provisionally accepted"): # <-- simple merge for accepted names
                matched_record["subspecies"] = matched_record["raw_name"]
                matched_record["subspecies_COL_code"] = matched_record["match_id"]
                for key, new_value in matched_record.items():

                    # If not a tax name, simply add data from matched record
                    if (key in non_tax_keys) or ("_COL_code" in key):
                        combined_record[key] = new_value
                        continue
                    
                    # Keep track of reclassifications
                    for old_key, old_value in combined_record.items():
                        if (old_value == new_value) and (old_key != key) and ("_COL_code" not in old_key) and (old_key not in non_tax_keys):
                            combined_record[old_key] = None

                            if id not in tax_reclass_log:
                                tax_reclass_log[id] = []
                            tax_reclass_log[id].append(f"{old_value} reclassified from {old_key} to {key}")
                    
                    old_value = combined_record.get(key)
                    if new_value != old_value:

                        # Tax updates
                        if old_value != None: 
                            if id not in tax_update_log:
                                tax_update_log[id] = []
                            tax_updated.append(key)
                            tax_update_log[id].append(f"{key} changed from '{old_value}' to '{new_value}'")
                            if key in higher_tax_list:
                                if id not in high_tax_update_log:
                                    high_tax_update_log[id] = []
                                high_tax_update_log[id].append(f"WARNING: {key} changed from '{old_value}' to '{new_value}'")

                        # Tax fills      
                        if old_value == None:
                            if id not in tax_fill_log:
                                tax_fill_log[id] = []
                            tax_filled.append(key)
                            tax_fill_log[id].append(f"{key} classification '{new_value}' added")
                        combined_record[key] = new_value

                combined_record["date_last_modified"] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                combined_record["tax_filled"] = tax_filled
                combined_record["tax_updated"] = tax_updated
                merged_data.append(combined_record)
            
            elif matched_record['status'] == "synonym": 
                if matched_record.get("subspecies") == None: # For when subspecies name is synonymous with species name
                    combined_record["species_synonyms"] = combined_record["species"]
                else:
                    combined_record["subspecies_synonyms"] = combined_record["species"] # For when subspecies name has subspecies synonym

                for key, new_value in matched_record.items():

                    # If not a tax name, simply add data from matched record
                    if (key in non_tax_keys) or ("_COL_code" in key):
                        combined_record[key] = new_value
                        continue
                    
                    # Keep track of reclassifications
                    for old_key, old_value in combined_record.items():
                        if (old_value == new_value) and (old_key != key) and ("_COL_code" not in old_key) and (old_key not in non_tax_keys):
                            combined_record[old_key] = None

                            if id not in tax_reclass_log:
                                tax_reclass_log[id] = []
                            tax_reclass_log[id].append(f"{old_value} reclassified from {old_key} to {key}")
                    
                    old_value = combined_record.get(key)
                    if new_value != old_value:

                        # Tax updates
                        if old_value != None: 
                            if id not in tax_update_log:
                                tax_update_log[id] = []
                            tax_updated.append(key)
                            tax_update_log[id].append(f"{key} changed from '{old_value}' to '{new_value}'")
                            if key in higher_tax_list:
                                if id not in high_tax_update_log:
                                    high_tax_update_log[id] = []
                                high_tax_update_log[id].append(f"WARNING: {key} changed from '{old_value}' to '{new_value}'")

                        # Tax fills      
                        if old_value == None:
                            if id not in tax_fill_log:
                                tax_fill_log[id] = []
                            tax_filled.append(key)
                            tax_fill_log[id].append(f"{key} classification '{new_value}' added")
                        combined_record[key] = new_value

                combined_record["date_last_modified"] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                combined_record["tax_filled"] = tax_filled
                combined_record["tax_updated"] = tax_updated
                merged_data.append(combined_record)

        ## FAMILY-LEVEL MATCHES
        elif matched_record["query_rank"] == matched_record['match_rank'] == 'family':
            matched_record["family"] = matched_record["query_name"]
            matched_record["family_COL_code"] = matched_record['match_id']
            combined_record = old_record.copy()

            #  Merge for accepted name match
            if matched_record["status"] in ("accepted","provisionally accepted"):
                for key, new_value in matched_record.items():

                    # If not a tax name, simply add data from matched record
                    if (key in non_tax_keys) or ("_COL_code" in key):
                        combined_record[key] = new_value
                        continue
                    
                    # Keep track of reclassifications
                    for old_key, old_value in combined_record.items():
                        if (old_value == new_value) and (old_key != key) and ("_COL_code" not in old_key) and (old_key not in non_tax_keys):
                            combined_record[old_key] = None

                            if id not in tax_reclass_log:
                                tax_reclass_log[id] = []
                            tax_reclass_log[id].append(f"{old_value} reclassified from {old_key} to {key}")
                    
                    old_value = combined_record.get(key)
                    if new_value != old_value:

                        # Tax updates
                        if old_value != None: 
                            if id not in tax_update_log:
                                tax_update_log[id] = []
                            tax_updated.append(key)
                            tax_update_log[id].append(f"{key} changed from '{old_value}' to '{new_value}'")
                            if key in higher_tax_list:
                                if id not in high_tax_update_log:
                                    high_tax_update_log[id] = []
                                high_tax_update_log[id].append(f"WARNING: {key} changed from '{old_value}' to '{new_value}'")

                        # Tax fills      
                        if old_value == None:
                            if id not in tax_fill_log:
                                tax_fill_log[id] = []
                            tax_filled.append(key)
                            tax_fill_log[id].append(f"{key} classification '{new_value}' added")
                        combined_record[key] = new_value

                combined_record["date_last_modified"] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                combined_record["tax_filled"] = tax_filled
                combined_record["tax_updated"] = tax_updated
                merged_data.append(combined_record)

            # Family synonyms
            elif matched_record['status'] == "synonym":
                combined_record['family_synonyms'] = combined_record["family"] # Moving old name to "synonyms" column
                for key, new_value in matched_record.items():

                    # If not a tax name, simply add data from matched record
                    if (key in non_tax_keys) or ("_COL_code" in key):
                        combined_record[key] = new_value
                        continue
                    
                    # Keep track of reclassifications
                    for old_key, old_value in combined_record.items():
                        if (old_value == new_value) and (old_key != key) and ("_COL_code" not in old_key) and (old_key not in non_tax_keys):
                            combined_record[old_key] = None

                            if id not in tax_reclass_log:
                                tax_reclass_log[id] = []
                            tax_reclass_log[id].append(f"{old_value} reclassified from {old_key} to {key}")
                    
                    old_value = combined_record.get(key)
                    if new_value != old_value:

                        # Tax updates
                        if old_value != None: 
                            if id not in tax_update_log:
                                tax_update_log[id] = []
                            tax_updated.append(key)
                            tax_update_log[id].append(f"{key} changed from '{old_value}' to '{new_value}'")
                            if key in higher_tax_list:
                                if id not in high_tax_update_log:
                                    high_tax_update_log[id] = []
                                high_tax_update_log[id].append(f"WARNING: {key} changed from '{old_value}' to '{new_value}'")

                        # Tax fills      
                        if old_value == None:
                            if id not in tax_fill_log:
                                tax_fill_log[id] = []
                            tax_filled.append(key)
                            tax_fill_log[id].append(f"{key} classification '{new_value}' added")
                        combined_record[key] = new_value

                combined_record["date_last_modified"] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                combined_record["tax_filled"] = tax_filled
                combined_record["tax_updated"] = tax_updated
                merged_data.append(combined_record)

        else:
            combined_record["date_last_modified"] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
            merged_data.append(combined_record)
            
    print("Matched data merged with AGSD records")
 
    return(merged_data, tax_update_log, tax_fill_log, high_tax_update_log, tax_reclass_log)

def remove_unneeded_columns(merged_data):
    columns = ["name_authorship", "GNV_edit_distance", "GNV_required", "issues", "match_id", "match_rank", "match_type", "nidx", "query_name", "query_rank", "raw_name", "scientific_name", "source_key", "status", "unranked", "unranked_COL_code"]
    for record in merged_data:
        for key in columns:
            record.pop(key, None)
    return merged_data

def results_to_csv(output_file, results_list):

    columns = set()
    for result in results_list:
        columns.update(result.keys())
    
    columns_sorted = sorted(columns)

    with open(output_file, "w", newline='', encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns_sorted)
        w.writeheader()
        w.writerows(results_list)

    print(f"{output_file} saved to the current directory")

def log_to_txt(log, filename):
    os.makedirs("merge_log_files", exist_ok=True)
    with open(f"merge_log_files/{filename}", 'w') as file:
        for key, value in log.items():
            file.write(f"{key}: {value}\n")

if __name__ == "__main__":

    print("\n     " + "-"*38 + "\n      Welcome to the AGSD Taxonomy Updater \n     " + "-"*38 + "\n     Dylan Harding, 2025\n")

    AGSD_data = input("Please ensure the AGSD file you wish to check is in the same directory as this script, and enter the file name here: ")
    dataset = input("Please enter the key for the ChecklistBank dataset you wish to check against. All datasets, including annual CoL releases can be found on the ChecklistBank website. (Eg. Col annual checklist = 310463): ")

    username = "dylanharding"
    password = "mygbifpassword"
    
    user_key = fetch_user_key(username, password)

    AGSD_records = AGSD_data_extract(AGSD_data)

    matches, unmatches, match_issues, match_errors, tax_classification_list = tax_namematch(dataset, AGSD_records, "AGSD species")
    matches_with_source_keys = append_source_keys(dataset, matches)
    matches_with_sources = append_source_names(dataset, matches_with_source_keys)

    clean_matches, ambiguous_matches = ambiguous_match_extract(matches)
    all_unmatched = unmatches + ambiguous_matches

    family_matches, family_unmatches, family_match_issues, family_match_errors, higher_tax_class_list = family_namematch(dataset, all_unmatched, "all unmatched")
    family_match_with_keys = append_source_keys(dataset, family_matches)
    family_match_with_sources = append_source_names(dataset, family_match_with_keys)

    all_matches_with_sources = clean_matches + family_matches
    all_match_errors = match_errors + family_match_errors

    merged_data, tax_updates, tax_fills, high_tax_updates, tax_reclass_log = data_merger(AGSD_records, all_matches_with_sources)

    log_to_txt(tax_updates, "tax_update_log")
    log_to_txt(tax_fills, "tax_fill_log")
    log_to_txt(high_tax_updates, "high_tax_update_log")
    log_to_txt(tax_reclass_log, "tax_reclassification_log")

    print("Log file saved to 'merge_log_files' subfolder")

    final_data = remove_unneeded_columns(merged_data)

    date_str = time.strftime("%m_%Y")
    results_to_csv(f"low_order_matches_{date_str}.csv", matches_with_sources)
    results_to_csv(f"unmatched_records_{date_str}.csv", family_unmatches)
    results_to_csv(f"match_error_log_{date_str}.csv", all_match_errors)
    results_to_csv(f"family_level_matches_{date_str}.csv", family_matches)
    results_to_csv(f"genome_entries_updated_{date_str}.csv", final_data)



