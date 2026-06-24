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