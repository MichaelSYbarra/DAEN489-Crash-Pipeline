#cleaning_rules.py

#This file performs the actually cleaning steps.

#imports
import pandas as pd
import numpy as np
import json
import logging

#This function drops irrelevant cols
def drop_garbage(data):

    #We have a lot of cols that aren't relevant to the current use case
    cols_to_drop = ["veh_unit_no_list_json", "veh_make_list_json","ppl_person_id_list_json","ppl_sex_list_json",
                    "injuries_total", "veh_vehicle_year_list_json", "ppl_injury_classification_list_json",
                    "ppl_safety_equipment_list_json", "ppl_airbag_deployed_list_json"
                    ]

    #We could potentially convert our prediction label to ppl_injury_classification_list_json for more granular injury / crahs serverity prediction.

    #Other things about injuries in the crash aren't important as our main outcome variable is crash_type
    #which we are using to sum up the severity of the crash. It's not very granular but we are essentially
    #trying to determine if road states and which road states contribute to making crashes more dangerous
    #in this case if they are the no drive away type due to injury / car damage.


    #Get rid of them
    data = data.drop(columns=cols_to_drop, axis = 1)

    return data


#Let's convert the lighting data into an ordinal number
def convert_light(data):
    #Lower number is less light so bigger number the better(more light)
    #We know what the values are so let's make a mapping operation

    mapping = {'DARKNESS':0,
               'DARKNESS, LIGHTED ROAD':1,
               'DAWN':2,
               'DUSK':2, #Let's just decided that Dawn and Dusk are the same
               'DAYLIGHT':3,
               'UNKNOWN':np.nan} #Unknown let's set to NaN for now
    
    data["lighting_condition"] = data["lighting_condition"].map(mapping)

    return data


#Parses the date line into different rows.
# years month day is_weekend and hour bin
def parse_date(data):
    
    # HERE---------------------------------------------------
    '''
    Potentially add a check to see if the column exists? Maybe drop column if no date or fill with NaN or 0's or some default values like Jan 1st 2000 1 am.
    '''


    #Don't reinvent the wheel, we got a function for this in pandas
    #Use the given coerce for some basic errors
    data["crash_date"] = pd.to_datetime(data["crash_date"], errors="coerce")

    #Split off the day information
    data["year"] = data["crash_date"].dt.year
    data["month"] = data["crash_date"].dt.month
    data["day"] = data["crash_date"].dt.day

    #Boolean 
    data["is_weekend"] = data["crash_date"].dt.weekday >= 5

    #Hour
    data["hour"] = data["crash_date"].dt.hour

    #Hour bin as requested by deep-cleaning.md
    bins = [0, 6, 12, 18, 24]
    labels = ["0-5", "6-11", "12-17", "18-23"]
    data["hour_bin"] = pd.cut(data["hour"], bins=bins, labels=labels, right=False)

    return data



# This function converts the crash type column to a 0 or a 1,
# This maps the text column to a trainable paramater
# Values not of the two specific types determined from an eda are set to NaN.
def type_to_binary(data):

    # Hard code in these
    mapping = {
    "INJURY AND / OR TOW DUE TO CRASH": 1,
    "NO INJURY / DRIVE AWAY": 0
    }

    # This should leave non-conforming lines as NaNs
    # One thing to consider could be fuzzy matching for typo issues? Passing on that for now.
    data['crash_type_binary'] = data['crash_type'].map(mapping)
    data = data.drop("crash_type", axis = 1) #Get rid of the old column


    #Alright as per deep-cleaning.md we gotta ditch rows with NaN output labels

    before_count = len(data)

    # Drop rows with NaN in the label column
    data = data.dropna(subset=['crash_type_binary'])

    after_count = len(data)

    # Compute percentage dropped
    dropped = before_count - after_count
    percent_dropped = (dropped / before_count) * 100

    logging.info(
        f"Dropped {dropped} rows out of {before_count} "
        f"({percent_dropped:.2f}%) due to missing crash_type_binary label."
    )

    print(f"Remaining rows: {after_count}")

    return data



def clean_traff_way_type(data):
    print("Go see what this data looks like")

# This is a supporter function for converting those json lists to numpy arrays
def parse_to_array(x):
        if pd.isna(x) or not isinstance(x, str):
            return np.array([])  # empty array if missing
        try:
            parsed = json.loads(x)
            if isinstance(parsed, list):
                return np.array(parsed)
            else:
                return np.array([])  # not a list? ignore
        except json.JSONDecodeError:
            return np.array([])  # bad JSON? skip graceful

#Helper function for contains
def contains_any(lst, terms):
    return int(any(item in terms for item in lst))
#A more specific helper function
def contains_bicycle(lst):
    return int("BICYCLE" in lst or "NON-MOTOR VEHICLE" in lst)
#Helper function for age
def to_filtered_array(arr):
        clean_nums = []
        for a in arr:
            try:
                a = float(a)  # Convert to number
                if 10 <= a <= 110:  # Filter range
                    clean_nums.append(a)
            except (ValueError, TypeError):
                continue  # Skip anything that can’t be converted
        return np.array(clean_nums) if clean_nums else np.nan

#Secondary parse to array helper

def parse_to_array_2(x):
    """Safely parse a JSON-style list string into a Python list."""
    if pd.isna(x):
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        try:
            parsed = json.loads(x)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
    return []

# This function is for the general json lists data that we need to aggregate into workable values.
def aggregate(data):

    #People -------------------------------------------------------------------------
    # We want to split into three columns.
    # Mean_age, max_age, min_age
    data["age"] = data["ppl_age_list_json"].apply(parse_to_array)
    
    #Drop the json lits column
    data = data.drop("ppl_age_list_json", axis = 1)

    #Clip ages to be within the range specified in deep-cleaning.md
    data["age"] = data["age"].apply(to_filtered_array)

    #Now that we have the array we want to make the other columns
    #Set to NaN if parse to array gave us an empty numpy array
    
    data["age_min"] = data["age"].apply(
        lambda x: np.min(x) if isinstance(x, (np.ndarray, list)) and len(x) > 0 else np.nan)
    data["age_max"] = data["age"].apply(
        lambda x: np.max(x) if isinstance(x, (np.ndarray, list)) and len(x) > 0 else np.nan)
    data["age_mean"] = data["age"].apply(
        lambda x: np.mean(x) if isinstance(x, (np.ndarray, list)) and len(x) > 0 else np.nan)
    #And finally drop the now unnecesary array of ages
    data = data.drop("age", axis = 1)

    #Vehicles -----------------------------------------------------------------------
    
    #Let's do veh_count first
    #Clip it to max 5 and min 1
    data["veh_count"] = data["veh_count"].clip(upper=5)
    data["veh_count"] = data["veh_count"].clip(lower=1)

    # Now it's time to collapse vehicle use list into 3 columns.
    emergency_terms = [
        "POLICE", "FIRE", "AMBULANCE", "TOW TRUCK", "CTA", "STATE OWNED"
    ]
    commercial_terms = [
        "COMMERCIAL - SINGLE UNIT", "COMMERCIAL - MULTI-UNIT",
        "RIDESHARE SERVICE", "TAXI/FOR HIRE", "CONSTRUCTION/MAINTENANCE",
        "AGRICULTURE", "HOUSE TRAILER"
    ]
    personal_terms = ["PERSONAL", "CAMPER/RV - SINGLE UNIT"]

    data["veh_vehicle_use_list_json"] = data["veh_vehicle_use_list_json"].apply(parse_to_array_2)

    # Create new indicator columns
    data["has_emergency"] = data["veh_vehicle_use_list_json"].apply(lambda x: contains_any(x, emergency_terms))
    data["has_commercial"] = data["veh_vehicle_use_list_json"].apply(lambda x: contains_any(x, commercial_terms))
    data["has_personal"] = data["veh_vehicle_use_list_json"].apply(lambda x: contains_any(x, personal_terms))

    data = data.drop(columns=["veh_vehicle_use_list_json"], axis = 1)
    
    #Relevant to this is another row, we can make a column for has_bicycle
    data["ppl_person_type_list_json"] = data["ppl_person_type_list_json"].apply(parse_to_array_2)


    data["has_bicycle"] = data["ppl_person_type_list_json"].apply(contains_bicycle)

    data = data.drop(columns=["ppl_person_type_list_json"], axis = 1)

    return data


# Call this function in cleaner.py to run the actual cleaning
def run_cleaning(file = "merged.csv"):
    print("Beginning")
    logging.info("Beginning Cleaning of merged.csv")

    merged = pd.read_csv(file)

    merged = drop_garbage(merged)

    merged = convert_light(merged)

    merged = parse_date(merged)

    merged = type_to_binary(merged)

    merged = aggregate(merged)

    #Once we complete all the above cleaning we will save the csv
    merged.to_csv("cleaned.csv", index=False)


#This is just here so if I do feel like running just this file we can but I don't think I ever will
#since this file depends on the running of the other services
if __name__ == "__main__":
    run_cleaning()