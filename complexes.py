"""
ComplexesData - A class for managing MTA station complex data.

This module provides functionality to work with MTA station complexes, which are groups
of stations that are physically connected and allow for transfers between different
subway lines. For example, Times Square-42 St is a complex that includes multiple
platforms and entrances.

The class loads data from two sources:
1. Complexes.csv - Contains information about station complexes and their GTFS stop IDs
2. GTFS stops.txt - Contains detailed information about each stop/station

Example usage:
    >>> complexes = ComplexesData()
    >>> # Get complex ID for a station
    >>> complex_id = complexes.get_complex_id_by_gtfs_stop_id("A34N")
    >>> # Get station names in a complex
    >>> names = complexes.get_names_of_stations(complex_id)
    >>> # Get all GTFS stop IDs in a complex
    >>> stop_ids = complexes.get_gtfs_stop_ids_by_complex_id(complex_id)
"""

# Quick Summary:
# ------------
# ComplexesData maps between:
# - GTFS stop IDs (e.g., "A34N") and complex IDs (e.g., "618")
# - Station names and their various identifiers
# - Multiple platforms/entrances within the same station complex
# Handles both directional (N/S) and non-directional stop IDs

import csv
import pandas as pd

class ComplexesData:
    def __init__(self, csv_path: str = 'data/Complexes.csv', gtfs_dir: str = 'data/gtfs_subway'):
        self.complex_id_by_gtfs = {}
        self.complex_info = {}
        
        # Load GTFS stops data
        self._stops = pd.read_csv(f"{gtfs_dir}/stops.txt")
        self.stop_name_map = dict(zip(self._stops.stop_id, self._stops.stop_name))

        with open(csv_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                complex_id = row['Complex ID']
                num_stations = int(row['Number Of Stations In Complex'])
                gtfs_stop_ids = row['GTFS Stop IDs']
                # Some rows have multiple GTFS stop ids separated by semicolons
                gtfs_list = [gtfs.strip() for gtfs in gtfs_stop_ids.split(';') if gtfs.strip()]
                self.complex_info[complex_id] = {
                    'num_stations': num_stations,
                    'gtfs_stop_ids': gtfs_list
                }
                # Store mapping for both with and without direction suffix
                for gtfs in gtfs_list:
                    self.complex_id_by_gtfs[gtfs] = complex_id
                    # Also store without direction suffix
                    if gtfs[-1] in ['N', 'S']:
                        self.complex_id_by_gtfs[gtfs[:-1]] = complex_id

    def get_complex_id_by_gtfs_stop_id(self, gtfs_stop_id):
        """Return the complex id for a given GTFS stop id, or None if not found.
        
        Parameters
        ----------
        gtfs_stop_id : str
            GTFS stop ID, with or without direction suffix (e.g. "A34" or "A34N")
        """
        # Try exact match first
        complex_id = self.complex_id_by_gtfs.get(gtfs_stop_id)
        if complex_id:
            return complex_id
            
        # If not found and has direction suffix, try without it
        if gtfs_stop_id[-1] in ['N', 'S']:
            return self.complex_id_by_gtfs.get(gtfs_stop_id[:-1])
            
        # If not found and no direction suffix, try with N
        return self.complex_id_by_gtfs.get(gtfs_stop_id + 'N')

    def get_number_of_stations(self, complex_id):
        """Return the number of stations in a complex, or None if not found."""
        info = self.complex_info.get(str(complex_id))
        if info:
            return info['num_stations']
        return None

    def get_gtfs_stop_ids_by_complex_id(self, complex_id):
        """Return a list of GTFS stop ids for a complex, or None if not found."""
        info = self.complex_info.get(str(complex_id))
        if info:
            return info['gtfs_stop_ids']
        return None

    def get_names_of_stations(self, complex_id):
        """
        Return a list of station names for a given complex ID.
        
        Parameters
        ----------
        complex_id : str
            The complex ID to look up
            
        Returns
        -------
        list[str] | None
            List of station names in the complex, or None if complex not found
        """
        gtfs_stop_ids = self.get_gtfs_stop_ids_by_complex_id(complex_id)
        if not gtfs_stop_ids:
            return None
            
        station_names = []
        for stop_id in gtfs_stop_ids:
            name = self.stop_name_map.get(stop_id)
            if name:
                station_names.append(name)
                
        return station_names

    def get_station_name(self, complex_id):
        """Return the primary station name for a complex ID."""
        names = self.get_names_of_stations(complex_id)
        return names[0] if names else "Unknown"

    def get_station_name_by_gtfs_id(self, gtfs_stop_id: str) -> str | None:
        """Get the station name for a GTFS stop ID.
        
        Parameters
        ----------
        gtfs_stop_id : str
            The GTFS stop ID (e.g., "A34N" or "A34")
            
        Returns
        -------
        str | None
            The station name if found, None otherwise
            
        Example
        -------
        >>> complexes = ComplexesData()
        >>> complexes.get_station_name_by_gtfs_id("A34N")
        'Times Square-42 St'
        """
        # Try exact match first
        name = self.stop_name_map.get(gtfs_stop_id)
        if name:
            return name
            
        # If not found and has direction suffix, try without it
        if gtfs_stop_id[-1] in ['N', 'S']:
            return self.stop_name_map.get(gtfs_stop_id[:-1])
            
        # If not found and no direction suffix, try with N
        return self.stop_name_map.get(gtfs_stop_id + 'N')

# Example usage:
# complexes = ComplexesData('data/Complexes.csv')
# print(complexes.get_complex_id_by_gtfs_stop_id('L03'))
# print(complexes.get_number_of_stations('602'))
# print(complexes.get_gtfs_stop_ids_by_complex_id('602'))
# print(complexes.get_names_of_stations('602')) 