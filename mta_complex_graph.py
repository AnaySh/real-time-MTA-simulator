"""
MTAComplexGraph - An undirected graph representation of the NYC subway system.

This module provides functionality to build and query a graph representation of the
NYC subway system, where:
- Nodes represent station complexes (groups of physically connected stations)
- Edges represent direct connections between stations on the same line
- Edge attributes store which subway lines connect the stations
- The graph is undirected, meaning connections work both ways

The graph is built using data from GTFS files and station complex information.
"""

from pathlib import Path
import pandas as pd
import networkx as nx
from complexes import ComplexesData

class MTAComplexGraph:
    """An undirected graph representation of the NYC subway system.
    
    This class provides a graph-based representation of the subway system where:
    - Nodes are station complexes (identified by complex IDs like "618")
    - Edges represent direct connections between stations on the same line
    - Edge attributes store which subway lines connect the stations
    - The graph is undirected, meaning connections work both ways
    """
    
    # Class-level caches
    G: nx.Graph | None = None          # the undirected graph itself
    _stops: pd.DataFrame | None = None
    _trips: pd.DataFrame | None = None
    _stop_times: pd.DataFrame | None = None
    _complexes: ComplexesData | None = None

    @classmethod
    def build_graph(cls, gtfs_dir: str | Path = "data/gtfs_subway") -> None:
        """Build the subway graph from GTFS data and station complex information.
        
        This method creates an undirected graph where:
        - Each node is a station complex (e.g., "618" for Times Square)
        - Node attributes include the station name and GTFS stop IDs
        - Edges connect stations that are directly reachable on the same line
        - Edge attributes list which subway lines connect the stations
        
        Parameters:
            gtfs_dir (str | Path): Directory containing GTFS data files
        """
        gtfs_dir = Path(gtfs_dir)

        # Initialize complexes data
        cls._complexes = ComplexesData()

        # Load GTFS data
        cls._stops = pd.read_csv(gtfs_dir / "stops.txt")
        cls._trips = pd.read_csv(gtfs_dir / "trips.txt")
        cls._stop_times = pd.read_csv(gtfs_dir / "stop_times.txt")

        # Get unique routes
        routes = cls._trips['route_id'].unique()
        
        # Initialize graph
        G = nx.Graph()

        # Process each route in both directions
        for route in routes:
            for direction in [0, 1]:
                try:
                    # Get ordered stops for this route and direction
                    stops = cls.ordered_stops(route, direction)
                    
                    # Convert GTFS stop IDs to complex IDs
                    complex_ids = []
                    for stop in stops:
                        complex_id = cls._complexes.get_complex_id_by_gtfs_stop_id(stop)
                        if complex_id:
                            complex_ids.append(complex_id)
                    
                    # Add all nodes with their names and GTFS IDs
                    for complex_id in complex_ids:
                        if complex_id not in G:
                            # Get all GTFS IDs for this complex
                            gtfs_ids = cls._complexes.get_gtfs_stop_ids_by_complex_id(complex_id)
                            # Get station name
                            station_name = cls._complexes.get_station_name(complex_id)
                            G.add_node(complex_id, 
                                     stop_name=station_name,
                                     gtfs_ids=gtfs_ids)
                    
                    # Create edges between consecutive stops
                    for i in range(len(complex_ids) - 1):
                        u = complex_ids[i]
                        v = complex_ids[i + 1]
                        
                        # Add or update edge
                        if G.has_edge(u, v):
                            if route not in G[u][v]['lines']:
                                G[u][v]['lines'].append(route)
                        else:
                            G.add_edge(u, v, lines=[route])
                except Exception as e:
                    print(f"Warning: Could not process route {route} direction {direction}: {e}")

        # Sort the lines list for each edge
        for u, v, data in G.edges(data=True):
            data['lines'] = sorted(data['lines'])

        cls.G = G

    @classmethod
    def _assert_built(cls):
        """Raise an error if the graph hasn't been built yet."""
        if cls.G is None:
            raise RuntimeError("Call MTAComplexGraph.build_graph() first.")

    @classmethod
    def ordered_stops(cls, route: str, direction: int) -> list[str]:
        """Get the ordered list of stops for a subway line.
        
        This returns the sequence of stops in the order they appear on the
        timetable for a specific route and direction.
        
        Parameters:
            route (str): The subway line (e.g., "A", "L", "1")
            direction (int): 0 for north/east-bound, 1 for south/west-bound
            
        Returns:
            list[str]: List of complex IDs in order
            
        Example:
            >>> MTAComplexGraph.ordered_stops("A", 0)
            ['H01', 'H02', 'H03', ...]  # Complex IDs for A train stops
        """
        cls._assert_built()
        trip_id = cls._trips.query(
            "route_id==@route and direction_id==@direction"
        ).trip_id.iloc[0]
        
        # Get GTFS stop IDs in order
        gtfs_stops = (
            cls._stop_times.query("trip_id==@trip_id")
                           .sort_values("stop_sequence")
                           .stop_id.tolist()
        )
        
        # Convert GTFS stop IDs to complex IDs
        complex_ids = []
        for stop in gtfs_stops:
            complex_id = cls._complexes.get_complex_id_by_gtfs_stop_id(stop)
            if complex_id and complex_id not in complex_ids:  # Avoid duplicates
                complex_ids.append(complex_id)
                
        return complex_ids

    @classmethod
    def connecting_lines(cls, complex_id_1: str, complex_id_2: str) -> list[str]:
        """Get all subway lines that directly connect two stations.
        
        Parameters:
            complex_id_1 (str): Complex ID of the first station
            complex_id_2 (str): Complex ID of the second station
            
        Returns:
            list[str]: List of subway lines that connect the stations
        """
        cls._assert_built()
        if cls.G.has_edge(complex_id_1, complex_id_2):
            return cls.G[complex_id_1][complex_id_2]['lines']
        return []

    @classmethod
    def shortest_path(cls, complex_id_1: str, complex_id_2: str) -> tuple[list[str], list[list[str]]]:
        """Find the shortest path between two stations and the connecting lines.
        
        Parameters:
            complex_id_1 (str): Complex ID of the starting station
            complex_id_2 (str): Complex ID of the destination station
            
        Returns:
            tuple[list[str], list[list[str]]]: 
                - List of complex IDs representing the path
                - List of lists of subway lines connecting each pair of stations
                
        Example:
            >>> path, lines = MTAComplexGraph.shortest_path("618", "164")
            >>> print(f"Path: {path}")
            >>> print(f"Lines: {lines}")
        """
        cls._assert_built()
        try:
            # Get the shortest path
            path = nx.shortest_path(cls.G, source=complex_id_1, target=complex_id_2)
            
            # Get the lines for each segment
            lines = []
            for i in range(len(path) - 1):
                u = path[i]
                v = path[i + 1]
                lines.append(cls.G[u][v]['lines'])
            
            return path, lines
        except nx.NetworkXNoPath:
            return [], []

# Example usage:
if __name__ == "__main__":
    MTAComplexGraph.build_graph()
    print("Graph built with", len(MTAComplexGraph.G), "stations")
    
    # Example: Find path from Times Square to Union Square
    path, lines = MTAComplexGraph.shortest_path("618", "164")
    print("\nPath from Times Square to Union Square:")
    for i, (station, station_lines) in enumerate(zip(path, lines)):
        station_name = MTAComplexGraph.G.nodes[station]['stop_name']
        if i < len(lines):
            print(f"{station_name} ({station}) -> Take {', '.join(station_lines)}")
        else:
            print(f"{station_name} ({station})") 