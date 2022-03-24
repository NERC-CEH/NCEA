def get_EA_WQ_determinands_for_site(site_id):
    """
    Get determinands for site.

    """
    url = "%s/id/sampling-point/%s/determinands.json" % (paths.EA_WQ_API_URL,
                                                         site_id)

    determinands = None
    with urllib.request.urlopen(url) as response:
        data = json.load(response)

    return data['items']


def _get_EA_WQ_measurement_availability(site_id, det_id, get_dtype_dict=False):
    """
    Get the data availability of given determinand for this site.

    """
    limit = 500
    offset = 0

    measures_dict = None
    dtype_dict = None
    finished = False
    first_call = True
    measure_values = []
    while finished is False:
        # Query string
        materials = "&sampledMaterialType=".join(EA_WQ_MATERIALS)
        query = "_limit=%s&_offset=%s&samplingPoint=%s&determinand=%s&" \
                "sampledMaterialType=%s" % (limit, offset, site_id, det_id,
                                            materials)
        # Full URL
        url = "%s/data/measurement?%s" % (paths.EA_WQ_API_URL, query)

        with urllib.request.urlopen(url) as response:
            print("Measurement call %s to %s for %s" % (offset,
                                                        offset + limit,
                                                        site_id))
            data = json.load(response)

            # Check if response has less than limit, indicating the end of
            # available sites
            if len(data["items"]) < limit:
                finished = True

            for measure_info in data["items"]:
                sample_date = datetime.strptime(
                    measure_info["sample"]["sampleDateTime"],
                    EA_WQ_API_DATE_FORMAT)

                # Collect the actual data
                measure_values.append(measure_info["result"])

                if get_dtype_dict is True:
                    dtype_dict = make_dtype_dict(
                        dtype_id=det_id,
                        dtype_name=measure_info["determinand"]["label"],
                        network_id=config.EA_WQ_ID,
                        dtype_desc=measure_info["determinand"]["definition"],
                        units=measure_info["determinand"]["unit"]["label"])

                if measures_dict is None:
                    measures_dict = make_avail_dict(site_id=site_id,
                                                    network_id=config.EA_WQ_ID,
                                                    dtype_id=det_id,
                                                    start_date=sample_date,
                                                    end_date=sample_date)

                else:
                    if sample_date < measures_dict["START_DATE"]:
                        measures_dict["START_DATE"] = sample_date
                    elif sample_date > measures_dict["END_DATE"]:
                        measures_dict["END_DATE"] = sample_date

        offset += limit

    if measures_dict is not None:
        measures_dict["SITE_VALUE_MEAN"] = round(np.mean(measure_values), 2)
        measures_dict["SITE_VALUE_COUNT"] = len(measure_values)

    return measures_dict, dtype_dict


def create_EA_WQ_metadata_old(limit_calls=None):
    """
    Create metatdata on the sites, data types and data availability for EA
    water quality (EA_WQ).
    We just look at data type info for nitrate and phosphate.
    Data is collected from the API and saved to CSV.

    """
    print("EA_WQ metadata")
    # Site and data metadata --------------------------------------------------
    # Set up coordinate transformation
    transformer = Transformer.from_crs("EPSG:27700", "EPSG:4326")

    limit = 500
    offset = 0

    dtype_rows = {}

    finished = False
    first_call = True
    calls = 0
    while finished is False:
        sites_rows = []
        avail_rows = []

        # Query string
        query = "_limit=%s&_offset=%s" % (limit, offset)
        # Full URL
        url = "%s/id/sampling-point?%s" % (paths.EA_WQ_API_URL, query)
        with urllib.request.urlopen(url) as response:
            print("Sites call %s to %s" % (offset, offset + limit))
            calls += 1
            data = json.load(response)

            # Check if response has less than limit, indicating the end of
            # available sites.
            if len(data["items"]) < limit:
                finished = True

            if limit_calls is not None and limit_calls == calls:
                finished = True

            for site_info in data["items"]:
                lat, long = transformer.transform(site_info["easting"],
                                                  site_info["northing"])

                site = make_site_dict(site_id=site_info["notation"],
                                      site_name=site_info["label"],
                                      network_id=config.EA_WQ_ID,
                                      lat=lat,
                                      long=long)

                for dtype_id in EA_WQ_DTYE_IDS:
                    # Extract nitrate data
                    if dtype_id not in dtype_rows:
                        get_dtype_dict = True
                    else:
                        get_dtype_dict = False

                    # Create dtype dict as well.
                    (avail_row,
                     dtype_dict) = _get_EA_WQ_measurement_availability(
                        site_info["notation"], dtype_id, get_dtype_dict)

                    if avail_row is not None:
                        avail_rows.append(avail_row)
                    if dtype_dict is not None:
                        dtype_rows[dtype_id] = dtype_dict

                sites_rows.append(site)

        sites = pd.DataFrame(sites_rows)
        avail = pd.DataFrame(avail_rows)

        if first_call:
            # We want to overwrite any existing CSV
            sites.to_csv(paths.SITE_REGISTER_FPATH.format(
                             NETWORK=config.EA_WQ_ID),
                         na_rep=None,
                         index=False,
                         date_format=config.DATE_FORMAT)

            avail.to_csv(paths.DATA_AVAILABILITY_FPATH.format(
                             NETWORK=config.EA_WQ_ID),
                         na_rep=None,
                         index=False,
                         date_format=config.DATE_FORMAT)

            first_call = False

        else:
            # Append to exisiting CSV
            sites.to_csv(paths.SITE_REGISTER_FPATH.format(
                             NETWORK=config.EA_WQ_ID),
                         mode='a',
                         na_rep=None,
                         index=False,
                         header=False,
                         date_format=config.DATE_FORMAT)

            avail.to_csv(paths.DATA_AVAILABILITY_FPATH.format(
                             NETWORK=config.EA_WQ_ID),
                         mode='a',
                         na_rep=None,
                         index=False,
                         header=False,
                         date_format=config.DATE_FORMAT)

        offset += limit

    dtype_rows = _add_dtype_stats(avail_rows, dtype_rows)

    pd.DataFrame(dtype_rows.values()).to_csv(
        paths.DTYPE_REGISTER_FPATH.format(NETWORK=config.EA_WQ_ID),
        index=False,
        date_format=config.DATE_FORMAT)


def _update_min_max_means(dtype_dict, value_mean):
    if dtype_dict["MIN_VALUE_MEAN"] is None:
        # Add min value
        dtype_dict["MIN_VALUE_MEAN"] = value_mean
    elif dtype_dict["MIN_VALUE_MEAN"] > value_mean:
        # Update min value
        dtype_dict["MIN_VALUE_MEAN"] = value_mean

    if dtype_dict["MAX_VALUE_MEAN"] is None:
        # Add min value
        dtype_dict["MAX_VALUE_MEAN"] = value_mean
    elif dtype_dict["MAX_VALUE_MEAN"] < value_mean:
        # Update min value
        dtype_dict["MAX_VALUE_MEAN"] = value_mean

    return dtype_dict


def _extract_EA_BIO_metadata(site_info, data_types, transformer):
    """
    Extract site and data availability data from the site info dictionary
    returned in the API response.

    """
    lat, long = transformer.transform(site_info["easting"],
                                      site_info["northing"])

    # Site data
    site_id = site_info["local_id"]
    site = make_site_dict(site_id=site_id,
                          site_name=site_info["label"],
                          network_id=config.EA_BIO_ID,
                          lat=lat,
                          long=long)

    # Data availability
    dtype_avail = {}
    for prop in site_info["properties"]:
        # Split label string into listed words
        prop_label_words = prop["property_label"].split()
        # We only want properties to do with a particular data
        # type, i.e. INV, MACB or DIAT. These are given as last
        # word in property_label.
        dtype_id = prop_label_words[-1]
        if dtype_id not in data_types:
            continue

        # Next, the first word tells us the property type;
        # "Min"/"Max" for min.max date, or "Count" for value count
        if prop_label_words[0] == "Min":
            # Extract and reformat date string
            start_date = datetime.strptime(prop["value"],
                                           EA_BIO_API_DATE_FORMAT)

            # Create/add to row dictionary
            if dtype_id not in dtype_avail:
                dtype_avail[dtype_id] = make_avail_dict(
                    site_id=site_id,
                    network_id=config.EA_BIO_ID,
                    dtype_id=dtype_id,
                    start_date=start_date)
            else:
                dtype_avail[dtype_id]["START_DATE"] = start_date

        elif prop_label_words[0] == "Max":
            # Extract and reformat date string
            end_date = datetime.strptime(prop["value"], EA_BIO_API_DATE_FORMAT)

            # Create/add to row dictionary
            if dtype_id not in dtype_avail:
                dtype_avail[dtype_id] = make_avail_dict(
                    site_id=site_id,
                    network_id=config.EA_BIO_ID,
                    dtype_id=dtype_id,
                    end_date=end_date)
            else:
                dtype_avail[dtype_id]["END_DATE"] = end_date

        elif prop_label_words[0] == "Count":
            # Create/add to row dictionary
            if dtype_id not in dtype_avail:
                dtype_avail[dtype_id] = make_avail_dict(
                    site_id=site_id,
                    network_id=config.EA_BIO_ID,
                    dtype_id=dtype_id,
                    value_count=prop["value"])
            else:
                dtype_avail[dtype_id]["VALUE_COUNT"] = prop["value"]

    return site, list(dtype_avail.values())


def create_EA_BIO_FISH_metadata(limit_calls=None):
    """
    Fetch site, availability and data type info for EA Biosys (EA_BIO) and fish
    (EA_FISH) data and save to file.

    Note, these are treated as separate networks but are accessed through the
    same API.

    How data is delivered through the API differs for BioSys and fish.
    For BioSys we can extract all info from the "sites" call.
    However, for fish we must use both the "sites" call and the "surveys"
    calls.

    """
    network_id_base = "http://environment.data.gov.uk/ecology/def"
    bio_ntwk_id = "%s/BiosysFreshwaterSite" % network_id_base
    fish_ntwk_id = "%s/FishFreshwaterSite" % network_id_base

    # Set API paging settings
    limit = 500
    offset = 0

    # Create BioSys data types metadata
    bio_dtypes = {
        "INV": "Macroinvertebrates",
        "MACP": "Macrophyte",
        "DIAT": "Diatom",
    }
    bio_dtype_rows = []
    # Add BioSys data type metadata
    for dtype_id, dtype_name in bio_dtypes.items():
        bio_dtype_rows.append(make_dtype_dict(dtype_id=dtype_id,
                                              dtype_name=dtype_name,
                                              network_id=config.EA_BIO_ID))

    # Save data types
    pd.DataFrame(bio_dtype_rows).to_csv(
        paths.DTYPE_REGISTER_FPATH.format(NETWORK=config.EA_BIO_ID),
        index=False,
        date_format=config.DATE_FORMAT)

    # Add fish data type metadata
    fish_dtype_rows = [
        make_dtype_dict(dtype_id="FISH_COUNT",
                        dtype_name="Fish Count",
                        network_id=config.EA_FISH_ID,
                        dtype_desc="Fish count from surveys")
    ]
    pd.DataFrame(fish_dtype_rows).to_csv(
        paths.DTYPE_REGISTER_FPATH.format(NETWORK=config.EA_FISH_ID),
        index=False,
        date_format=config.DATE_FORMAT)

    # Set up coordinate transformation
    transformer = Transformer.from_crs("EPSG:27700", "EPSG:4326")

    fish_site_names = {}

    finished = False
    first_call = True
    calls = 0
    while finished is False:
        bio_sites_rows = []
        bio_avail_rows = []

        # Query string
        query = "take=%s&skip=%s&mode=props" % (limit, offset)
        # Full URL
        url = "%ssites?%s" % (paths.EA_ECO_BASE_URL, query)

        with urllib.request.urlopen(url) as response:
            print("Sites call %s to %s" % (offset, offset + limit))
            call += 1
            data = json.load(response)

            # Check if response has less than limit, indicating the end of
            # available sites
            if len(data) < limit:
                finished = True

            if limit_calls is not None and limit_calls == calls:
                finished = True

            for site_info in data:
                if site_info["type"] == bio_ntwk_id:
                    # EA Biosys site. Extract all data fom the site_info
                    site, dtype_avail = _extract_EA_BIO_metadata(
                        site_info, bio_dtypes, transformer)

                    bio_sites_rows.append(site)
                    bio_avail_rows += dtype_avail

                elif site_info["type"] == fish_ntwk_id:
                    # EA fish site. This only contains the site name for now.
                    fish_site_names[site_info["local_id"]] = site_info["label"]

        bio_sites = pd.DataFrame(bio_sites_rows)
        bio_avail = pd.DataFrame(bio_avail_rows)

        if first_call:
            # We want to overwrite any existing CSV
            bio_sites.to_csv(paths.SITE_REGISTER_FPATH.format(
                                NETWORK=config.EA_BIO_ID),
                             na_rep=None,
                             index=False,
                             date_format=config.DATE_FORMAT)

            bio_avail.to_csv(paths.DATA_AVAILABILITY_FPATH.format(
                                NETWORK=config.EA_BIO_ID),
                             na_rep=None,
                             index=False,
                             date_format=config.DATE_FORMAT)

            first_call = False

        else:
            # Append to exisiting CSV
            bio_sites.to_csv(paths.SITE_REGISTER_FPATH.format(
                                NETWORK=config.EA_BIO_ID),
                             mode='a',
                             na_rep=None,
                             index=False,
                             header=False,
                             date_format=config.DATE_FORMAT)

            bio_avail.to_csv(paths.DATA_AVAILABILITY_FPATH.format(
                                NETWORK=config.EA_BIO_ID),
                             mode='a',
                             na_rep=None,
                             index=False,
                             header=False,
                             date_format=config.DATE_FORMAT)

        offset += limit

    # Now we need to read through the survey data to extract more info on the
    # fish sites.
    offset = 0

    fish_sites_rows = {}
    fish_avail_rows = {}

    alt_coords_dict = {}

    finished = False
    first_call = True
    calls = 0
    while finished is False:
        # Query string
        query = "take=%s&skip=%s&site_type=%s" % (
            limit, offset, fish_ntwk_id)
        # Full URL
        url = "%ssurveys?%s" % (paths.EA_ECO_BASE_URL, query)

        with urllib.request.urlopen(url) as response:
            print("Surveys call %s to %s" % (offset, offset + limit))
            calls += 1
            data = json.load(response)

            # Check if response has less than limit, indicating the end of
            # available sites
            if len(data) < limit:
                finished = True

            if limit_calls is not None and limit_calls == calls:
                finished = True

            for surv_info in data:
                site_id = surv_info["site_id"].split("/")[-1]
                survey_date = datetime.strptime(surv_info["survey_date"],
                                                EA_BIO_API_DATE_FORMAT)

                lat = surv_info["survey_lat"]
                long = surv_info["survey_long"]

                # Sort site metadata
                if site_id not in fish_sites_rows:
                    site_name = fish_site_names.get(site_id)
                    if site_name is None:
                        print("No fish site name found for %s" % site_id)

                    fish_sites_rows[site_id] = make_site_dict(
                        site_id=site_id,
                        site_name=site_name,
                        network_id=config.EA_FISH_ID,
                        lat=lat,
                        long=long)
                else:
                    if fish_sites_rows[site_id]["LATITUDE"] != lat or \
                            fish_sites_rows[site_id]["LONGITUDE"] != long:
                        print("Fish site found with differing coordinates for "
                              "survey: %s" % site_id)
                        alt_coords = "(%s:%s)" % (lat, long)
                        if fish_sites_rows[site_id]["ALT_COORDS"] is None:
                            fish_sites_rows[site_id]["ALT_COORDS"] = [alt_coords]
                        elif alt_coords not in fish_sites_rows[site_id]["ALT_COORDS"]:
                            fish_sites_rows[site_id]["ALT_COORDS"].append(alt_coords)

                # Sort data availability
                if site_id not in fish_avail_rows:
                    fish_avail_rows[site_id] = make_avail_dict(
                        site_id=site_id,
                        network_id=config.EA_FISH_ID,
                        dtype_id="FISH_COUNT",
                        start_date=survey_date,
                        end_date=survey_date,
                        value_count=1)
                else:
                    if survey_date < fish_avail_rows[site_id]["START_DATE"]:
                        fish_avail_rows[site_id]["START_DATE"] = survey_date
                    elif survey_date > fish_avail_rows[site_id]["END_DATE"]:
                        fish_avail_rows[site_id]["END_DATE"] = survey_date

                    fish_avail_rows[site_id]["VALUE_COUNT"] += 1

        offset += limit

    # Switch any ALT_COORDS lists into strings
    for site_id in fish_sites_rows:
        if fish_sites_rows[site_id]["ALT_COORDS"] is not None:
            alt_coord_str = ";".join(fish_sites_rows[site_id]["ALT_COORDS"])
            fish_sites_rows[site_id]["ALT_COORDS"] = alt_coord_str

    fish_sites = pd.DataFrame(fish_sites_rows.values())
    fish_avail = pd.DataFrame(fish_avail_rows.values())

    if first_call:
        # Overwrite any existing CSV
        fish_sites.to_csv(paths.SITE_REGISTER_FPATH.format(
                            NETWORK=config.EA_FISH_ID),
                          na_rep=None,
                          index=False,
                          date_format=config.DATE_FORMAT)

        fish_avail.to_csv(paths.DATA_AVAILABILITY_FPATH.format(
                            NETWORK=config.EA_FISH_ID),
                          na_rep=None,
                          index=False,
                          date_format=config.DATE_FORMAT)

        first_call = False

    else:
        # Append to exisiting CSV
        fish_sites.to_csv(paths.SITE_REGISTER_FPATH.format(
                            NETWORK=config.EA_FISH_ID),
                          mode='a',
                          na_rep=None,
                          index=False,
                          header=False,
                          date_format=config.DATE_FORMAT)

        fish_avail.to_csv(paths.DATA_AVAILABILITY_FPATH.format(
                            NETWORK=config.EA_FISH_ID),
                          mode='a',
                          na_rep=None,
                          index=False,
                          header=False,
                          date_format=config.DATE_FORMAT)



EA_INV_DTYPE_DICT = {
    "BMWP_N_TAXA": {
        "name": "Number of taxa contributing to the BMWP index",
        "desc": None,
        "unit": "count",
    },
    "BMWP_TOTAL": {
        "name": "BMWP index total score",
        "desc": None,
        "unit": "total score",
    },
    "BMWP_ASPT": {
        "name": "BMWP index Average Score Per Taxon",
        "desc": None,
        "unit": "average score per taxon",
    },
    "CCI_N_TAXA": {
        "name": "Number of taxa contributing to the CCI index",
        "desc": None,
        "unit": "count",
    },
    "CCI_CS_TOTAL": {
        "name": "CCI index total score",
        "desc": None,
        "unit": "total score",
    },
    "CCI_ASPT": {
        "name": "CCI index Average Score Per Taxon",
        "desc": None,
        "unit": "average score per taxon",
    },
    "CSmax_CoS": {
        "name": "Conservation score of the rarest taxon in sample",
        "desc": "Part of the CCI index calculations - it displays the "
                "conservation score of the rarest taxon in the sample.",
        "unit": "conservation score",
    },
    "BMWP_CoS": {
        "name": "Conservation Score derived from the BMWP score",
        "desc": "Part of the CCI index calculations - it displays the "
                "Conservation Score derived from the BMWP score.",
        "unit": "conservation score",
    },
    "CCI_CoS": {
        "name": "Highest conservation score",
        "desc": "This displays the highest Conservation Score (from either "
                "the rarest taxon or the BMWP range) and is used in the final "
                "CCI calculation.",
        "unit": "conservation score",
    },
    "CCI": {
        "name": "Community Conservation Index",
        "desc": "The final Community Conservation Index.",
        "unit": "score",
    },
    "DEHLI_N_TAXA": {
        "name": "Number of taxa contributing to the DEHLI index",
        "desc": None,
        "unit": "count",
    },
    "DIS_TOTAL": {
        "name": "DEHLI index total score",
        "desc": None,
        "unit": "total score",
    },
    "DEHLI": {
        "name": "Final DEHLI index",
        "desc": None,
        "unit": "score",
    },
    "EPSI_ML_S_GRP": {
        "name": "Number of Mixed-level E-PSI sensitive taxa",
        "desc": None,
        "unit": "count",
    },
    "EPSI_ML_ALL_GRP": {
        "name": "Number of Mixed-level E-PSI sensitive and insensitive taxa",
        "desc": None,
        "unit": "count",
    },
    "EPSI_MIXED_LEVEL_SCORE": {
        "name": "Mixed-level E-PSI index",
        "desc": None,
        "unit": "score",
    },
    "EPSI_S_GRP": {
        "name": "Number of family-level PSI sensitive/fairly sensitive taxa",
        "desc": None,
        "unit": "count",
    },
    "EPSI_ALL_GRP": {
        "name": "Number of family-level PSI scoring taxa",
        "desc": None,
        "unit": "count",
    },
    "EPSI_FAMILY_SCORE": {
        "name": "Family-level PSI index",
        "desc": None,
        "unit": "score",
    },
    "LIFE_N_TAXA": {
        "name": "Number of taxa contributing to Family LIFE index",
        "desc": None,
        "unit": "count",
    },
    "LIFE_SCORES_TOTAL": {
        "name": "Family LIFE index total score",
        "desc": None,
        "unit": "total score",
    },
    "LIFE_FAMILY_INDEX": {
        "name": "Family LIFE index",
        "desc": None,
        "unit": "score",
    },
    "LIFE_SPECIES_N_TAXA": {
        "name": "Number of taxa contributing to the Species LIFE index",
        "desc": None,
        "unit": "count",
    },
    "LIFE_SPECIES_SCORES_TOTAL": {
        "name": "Species LIFE index total score",
        "desc": None,
        "unit": "total score",
    },
    "LIFE_SPECIES_INDEX": {
        "name": "Species LIFE index",
        "desc": None,
        "unit": "score",
    },
    "PSI_ML_AB": {
        "name": "Number of mixed-level PSI sensitive/fairly sensitive taxa",
        "desc": None,
        "unit": "count",
    },
    "PSI_ML_ABCD": {
        "name": "Number of mixed-level PSI scoring taxa",
        "desc": None,
        "unit": "count",
    },
    "PSI_MIXED_LEVEL_SCORE": {
        "name": "Mixed-level PSI index",
        "desc": None,
        "unit": "score",
    },
    "PSI_AB": {
        "name": "Number of family-level PSI sensitive/fairly sensitive taxa",
        "desc": None,
        "unit": "count",
    },
    "PSI_ABCD": {
        "name": "Number of family-level PSI scoring taxa",
        "desc": None,
        "unit": "count",
    },
    "PSI_FAMILY_SCORE": {
        "name": "Family-level PSI index",
        "desc": None,
        "unit": "score",
    },
    "WHPT_N_TAXA": {
        "name": "Number of taxa contributing to the WHPT index",
        "desc": None,
        "unit": "count",
    },
    "WHPT_TOTAL": {
        "name": "WHPT index total score",
        "desc": None,
        "unit": "total score",
    },
    "WHPT_ASPT": {
        "name": "WHPT index Average Score Per Taxon",
        "desc": None,
        "unit": "average score per taxon",
    },
    "WHPT_NW_N_TAXA": {
        "name": "Number of taxa contributing to the non-abundance weighted "
                "WHPT index",
        "desc": None,
        "unit": "count",
    },
    "WHPT_NW_TOTAL": {
        "name": "Non-abundance weighted WHPT index total score",
        "desc": "Note: Do not confuse this with the more commonly used WHPT "
                "index which includes abundance",
        "unit": "total score",
    },
    "WHPT_NW_ASPT": {
        "name": "Non-abundance weighted WHPT Average Score Per Taxon",
        "desc": None,
        "unit": "average score per taxon",
    },
}
