# -*- coding: utf-8 -*-
"""
Functions for retreiving and saving catchment data for network sites and
arbitory coordinates.

"""
import catchment_tools as c_tools
import paths

from pyproj import Transformer


def network_catchment_data(networks="all", desc_types="all"):
    """
    Get catchment data (FEH descriptors and LCM) for all sites at given
    networks and save to file.

    """
    if isinstance(networks, str):
        networks = [networks]
    if networks[0] == "all":
        networks = config.VALID_NETWORKS
    else:
        for ntwrk in networks:
            if ntwrk not in config.VALID_NETWORKS:
                raise UserWarning("%s is not a valid network. Choose from %s"
                                  % (ntwrk, ", ".join(config.VALID_NETWORKS)))

    transformer = Transformer.from_crs("EPSG:4326", "EPSG:27700")

    for network_id in networks:
        sites = pd.read_csv(
            paths.SITE_REGISTER_FPATH.format(NETWORK=network_id),
            dtype={"SITE_ID": str})

        for site_row in sites.iterrows():
            easting, northing = transformer.transform(site_row["LATITUDE"],
                                                      site_row["LONGITUDE"])
            catch = c_tools.CatchmentData(easting, northing,
                                          snap_to_river=True)
            data = catch.get_data(desc_types=desc_types)
