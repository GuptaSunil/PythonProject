def transform_data(rows):
    """Transform data with cleaning and enrichment."""
    transformed = []
    for row in rows:
       # if row.name:  # filter out empty names
            transformed.append((
                row.officename,
                row.pincode,
                row.officeType,
                row.Deliverystatus,
                row.divisionname,
                row.regionname, 
                row.circlename,
                row.Taluk,
                row.Districtname,
                row.statename,
                row.STATE_CD
            ))
    return transformed



def transform_Pincode(rows):
    """Transform data with cleaning and enrichment."""
    transformed = []
    for row in rows:
       # if row.name:  # filter out empty names
            transformed.append((
                row.officename,
                row.pincode,
                row.officeType,
                row.Deliverystatus,
                row.divisionname,
                row.regionname, 
                row.circlename,
                row.Taluk,
                row.Districtname,
                row.statename,
                row.STATE_CD
            ))
    return transformed

def transform_STATE_Code(rows):
    """Transform data with cleaning and enrichment."""
    transformed = []
    for row in rows:
       # if row.name:  # filter out empty names
            transformed.append((
                row.Seq_no,
                row.State_Name,
                row.STATE_Code,
                row.GST_STATE_Code
            ))
    return transformed


def transform_Pincode_Kafka(rows):
    transformed = []
    for row in rows:  # row is a dict
        transformed.append({
            "officename": row["officename"],
            "pincode": row["pincode"],
            "officeType": row["officeType"],
            "Deliverystatus": row["Deliverystatus"],
            "divisionname": row["divisionname"],
            "regionname": row["regionname"],
            "circlename": row["circlename"],
            "Taluk": row["Taluk"],
            "Districtname": row["Districtname"],
            "statename": row["statename"],
            "STATE_CD": row["STATE_CD"]
        })
    return transformed


def transform_STATE_Code_Kafka(rows):
    transformed = []
    for row in rows:  # row is a dict
        transformed.append({
            "Seq_no": row["Seq_no"],
            "State_Name": row["State_Name"],
            "STATE_Code": row["STATE_Code"],
            "GST_STATE_Code": row["GST_STATE_Code"]
        })
    return transformed
