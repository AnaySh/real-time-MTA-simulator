"""
SubwayGraph - A NetworkX-based representation of the NYC subway system.

This module provides functionality to build and query a graph representation of the
NYC subway system, where:
- Nodes represent station complexes (groups of physically connected stations)
- Edges represent direct connections between stations on the same line
- Edge attributes store which subway lines connect the stations

The graph is built using data from GTFS files and station complex information.
It provides methods to:
- Find paths between stations
- Identify connecting lines between stations
- Look up station names and IDs
- Get ordered lists of stations for each line

Example usage:
    >>> SubwayGraph.build_graph()
    >>> # Find stations connected to Times Square
    >>> successors = SubwayGraph.successors("618")  # 618 is Times Square's complex ID
    >>> # Find which lines connect two stations
    >>> lines = SubwayGraph.connecting_lines("618", "327")
    >>> # Find shortest path between stations
    >>> path = SubwayGraph.shortest_path("618", "164")  # Times Square to Union Square
    >>> # Get directions with subway lines
    >>> directions = SubwayGraph.get_directions("618", "164")
    >>> print(directions)
    >>> # Get all possible shortest paths
    >>> all_paths = SubwayGraph.all_shortest_paths("618", "164")
    >>> for path in all_paths:
    >>>     print(SubwayGraph.get_directions_for_path(path))
"""

from pathlib import Path
import itertools
import pandas as pd
import networkx as nx
from complexes import ComplexesData


class SubwayGraph:
    """A NetworkX-based representation of the NYC subway system.
    
    This class provides a graph-based representation of the subway system where:
    - Nodes are station complexes (identified by complex IDs like "618")
    - Edges represent direct connections between stations on the same line
    - Edge attributes store which subway lines connect the stations
    
    The graph is built using GTFS data and station complex information, providing
    a comprehensive view of the subway network that accounts for station complexes
    and multiple lines sharing the same tracks.
    
    Class Attributes:
        G (nx.DiGraph): The directed graph representing the subway system
        _stops (pd.DataFrame): GTFS stops data
        _trips (pd.DataFrame): GTFS trips data
        _stop_times (pd.DataFrame): GTFS stop_times data
        _complexes (ComplexesData): Station complex information
    """
    
    # ------------------------------------------------------------------ #
    # Class-level caches – populate once, re-use everywhere
    # ------------------------------------------------------------------ #
    G: nx.DiGraph | None = None          # the directed graph itself
    _stops: pd.DataFrame | None = None
    _trips: pd.DataFrame | None = None
    _stop_times: pd.DataFrame | None = None
    _complexes: ComplexesData | None = None

    # ------------------------------------------------------------------ #
    # Graph-building
    # ------------------------------------------------------------------ #
    @classmethod
    def build_graph(cls, gtfs_dir: str | Path = "data/gtfs_subway") -> None:
        """Build the subway graph from GTFS data and station complex information.
        
        This method creates a directed graph where:
        - Each node is a station complex (e.g., "618" for Times Square)
        - Node attributes include the station name and GTFS stop IDs
        - Edges connect stations that are directly reachable on the same line
        - Edge attributes list which subway lines connect the stations
        
        The graph is built by:
        1. Loading GTFS data and station complex information
        2. Processing each subway line in both directions
        3. Creating nodes for each station complex
        4. Creating edges between all pairs of stations on the same line
        
        Parameters:
            gtfs_dir (str | Path): Directory containing GTFS data files
            
        Example:
            >>> SubwayGraph.build_graph()
            >>> print(f"Graph built with {len(SubwayGraph.G)} stations")
            Graph built with 472 stations
        """
        gtfs_dir = Path(gtfs_dir)

        # Initialize complexes data
        cls._complexes = ComplexesData()

        # Load GTFS data
        cls._stops      = pd.read_csv(gtfs_dir / "stops.txt")
        cls._trips      = pd.read_csv(gtfs_dir / "trips.txt")
        cls._stop_times = pd.read_csv(gtfs_dir / "stop_times.txt")

        # Get unique routes
        routes = cls._trips['route_id'].unique()
        
        # Initialize graph
        G = nx.DiGraph()

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
                    
                    # Create edges between all pairs of stops in order
                    for i in range(len(complex_ids)):
                        for j in range(i + 1, len(complex_ids)):
                            u = complex_ids[i]
                            v = complex_ids[j]
                            
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

    # ------------------------------------------------------------------ #
    # Convenience helpers
    # ------------------------------------------------------------------ #
    @classmethod
    def _assert_built(cls):
        """Raise an error if the graph hasn't been built yet.
        
        This is a helper method used by other methods to ensure the graph
        exists before attempting to use it.
        
        Raises:
            RuntimeError: If build_graph() hasn't been called
        """
        if cls.G is None:
            raise RuntimeError("Call SubwayGraph.build_graph() first.")

    # ---------- 1. name ↔ complex_id look–ups ---------------------------
    @classmethod
    def stop_name_to_complex_id(cls, name: str) -> str | None:
        """Find the complex ID for a station name.
        
        Parameters:
            name (str): The station name to look up
            
        Returns:
            str | None: The complex ID if found, None otherwise
            
        Example:
            >>> SubwayGraph.stop_name_to_complex_id("Times Square-42 St")
            '618'
        """
        cls._assert_built()
        for node, data in cls.G.nodes(data=True):
            if data['stop_name'] == name:
                return node
        return None

    @classmethod
    def complex_id_to_name(cls, complex_id: str) -> str | None:
        """Get the station name for a complex ID.
        
        Parameters:
            complex_id (str): The complex ID to look up
            
        Returns:
            str | None: The station name if found, None otherwise
            
        Example:
            >>> SubwayGraph.complex_id_to_name("618")
            'Times Square-42 St'
        """
        cls._assert_built()
        return cls.G.nodes[complex_id]["stop_name"] if complex_id in cls.G else None

    @classmethod
    def lines_at_gtfs_stop_id(cls, gtfs_stop_id: str) -> list[str]:
        """Get all train lines that stop at a particular GTFS stop ID.
        
        Parameters:
            gtfs_stop_id (str): The GTFS stop ID (e.g., "A34" or "A34N")
            
        Returns:
            list[str]: Sorted list of unique train lines that stop at this GTFS stop
            
        Example:
            >>> SubwayGraph.lines_at_gtfs_stop_id("A34")
            ['A', 'C', 'E']
        """
        cls._assert_built()
        # Map GTFS stop ID to complex ID
        complex_id = cls._complexes.get_complex_id_by_gtfs_stop_id(gtfs_stop_id)
        if not complex_id or complex_id not in cls.G:
            return []

        # Aggregate all lines from edges connected to this complex
        lines = set()
        for _, v, data in cls.G.out_edges(complex_id, data=True):
            lines.update(data.get("lines", []))
        for u, _, data in cls.G.in_edges(complex_id, data=True):
            lines.update(data.get("lines", []))
        return sorted(lines)

    @classmethod
    def lines_at_complex_id(cls, complex_id: str) -> list[str]:
        """Get all train lines that stop at a particular station complex.
        
        Parameters:
            complex_id (str): The complex ID (e.g., "618" for Times Square)
            
        Returns:
            list[str]: Sorted list of unique train lines that stop at this complex
            
        Example:
            >>> SubwayGraph.lines_at_complex_id("618")
            ['A', 'C', 'E', 'L']
        """
        cls._assert_built()
        if not complex_id or complex_id not in cls.G:
            return []

        # Aggregate all lines from edges connected to this complex
        lines = set()
        for _, v, data in cls.G.out_edges(complex_id, data=True):
            lines.update(data.get("lines", []))
        for u, _, data in cls.G.in_edges(complex_id, data=True):
            lines.update(data.get("lines", []))
        return sorted(lines)

    # ---------- 2. ordered stop list for a line -------------------------
    @classmethod
    def ordered_stops(cls, route: str, direction: int) -> list[str]:
        """Get the ordered list of stops for a subway line.
        
        This returns the sequence of stops in the order they appear on the
        timetable for a specific route and direction.
        
        Parameters:
            route (str): The subway line (e.g., "A", "L", "1")
            direction (int): 0 for north/east-bound, 1 for south/west-bound
            
        Returns:
            list[str]: List of GTFS stop IDs in order
            
        Example:
            >>> SubwayGraph.ordered_stops("E", 0)
            ['E01N', 'A34N', 'A33N', ...]
        """
        if route == "A" or route == "4": # This is a lot more expensive, but it's the only way to get the correct order for these lines
            trips = cls._trips.query("route_id==@route and direction_id==@direction")
            trip_ids = set(trips.trip_id)
            # Filter stop_times just once
            stop_times = cls._stop_times[cls._stop_times.trip_id.isin(trip_ids)]
            # Group by trip_id and count stops
            trip_counts = stop_times.groupby("trip_id").size()
            if trip_counts.empty:
                return []
            # Get the trip_id with the most stops
            best_trip_id = trip_counts.idxmax()
            # Get the ordered stops for that trip
            ordered = (
                stop_times[stop_times.trip_id == best_trip_id]
                .sort_values("stop_sequence")
                .stop_id.tolist()
            )
            return ordered
        
        trip_id = cls._trips.query(
            "route_id==@route and direction_id==@direction"
        ).trip_id.iloc[0]
        return (
            cls._stop_times.query("trip_id==@trip_id")
            .sort_values("stop_sequence")
            .stop_id.tolist()
        )

    # ---------- 3. next stops ------------------------------------------
    @classmethod
    def successors(cls, complex_id: str) -> list[str]:
        """Get all stations directly reachable from a given station.
        
        Parameters:
            complex_id (str): The complex ID of the starting station
            
        Returns:
            list[str]: List of complex IDs for directly reachable stations
            
        Example:
            >>> SubwayGraph.successors("618")  # Times Square
            ['327', '619', '620']
        """
        cls._assert_built()
        return list(cls.G.successors(complex_id))

    # ---------- 4. lines on a segment ----------------------------------
    @classmethod
    def connecting_lines(cls, u: str, v: str) -> list[str]:
        """Get all subway lines that directly connect two stations.
        
        Parameters:
            u (str): Complex ID of the starting station
            v (str): Complex ID of the destination station
            
        Returns:
            list[str]: List of subway lines that connect the stations
            
        Example:
            >>> SubwayGraph.connecting_lines("618", "327")  # Times Square to Grand Central
            ['7', 'S']
        """
        cls._assert_built()
        return cls.G[u][v]["lines"] if cls.G.has_edge(u, v) else []

    @classmethod
    def shortest_path(cls, start: str, end: str) -> list[str] | None:
        """Find the shortest path between two stations.
        
        This method uses Dijkstra's algorithm to find the path with the fewest
        transfers between two stations. The path is returned as a list of complex IDs.
        
        Parameters:
            start (str): Complex ID of the starting station
            end (str): Complex ID of the destination station
            
        Returns:
            list[str] | None: List of complex IDs representing the path, or None if no path exists
            
        Example:
            >>> SubwayGraph.shortest_path("618", "164")  # Times Square to Union Square
            ['618', '619', '164']  # Path through 5th Ave-53rd St
        """
        cls._assert_built()
        try:
            return nx.shortest_path(cls.G, source=start, target=end)
        except nx.NetworkXNoPath:
            return None

    @classmethod
    def shortest_path_with_lines(cls, start: str, end: str) -> list[tuple[str, list[str]]] | None:
        """Find the shortest path between two stations with connecting lines.
        
        This method returns both the stations in the path and the subway lines
        that connect each pair of stations.
        
        Parameters:
            start (str): Complex ID of the starting station
            end (str): Complex ID of the destination station
            
        Returns:
            list[tuple[str, list[str]]] | None: List of (station, lines) tuples, or None if no path exists
            
        Example:
            >>> SubwayGraph.shortest_path_with_lines("618", "164")
            [('618', ['N', 'Q', 'R', 'W']), ('619', ['E', 'M']), ('164', [])]
        """
        path = cls.shortest_path(start, end)
        if not path:
            return None
            
        result = []
        for i in range(len(path) - 1):
            current = path[i]
            next_stop = path[i + 1]
            lines = cls.connecting_lines(current, next_stop)
            result.append((current, lines))
        result.append((path[-1], []))  # Add the last station with no outgoing lines
        return result

    @classmethod
    def get_directions(cls, start: str, end: str) -> str | None:
        """Get human-readable directions for traveling between two stations.
        
        This method returns a string describing the subway lines to take and
        where to transfer between stations.
        
        Parameters:
            start (str): Complex ID of the starting station
            end (str): Complex ID of the destination station
            
        Returns:
            str | None: Directions as a string, or None if no path exists
            
        Example:
            >>> SubwayGraph.get_directions("618", "164")
            'Start at Times Square-42 St (618)
             Take the N, Q, R, W trains to 5th Ave-53rd St (619)
             Transfer to the E, M trains to 14th St-Union Square (164)'
        """
        path_with_lines = cls.shortest_path_with_lines(start, end)
        if not path_with_lines:
            return None
            
        directions = []
        current_station = path_with_lines[0][0]
        current_name = cls.complex_id_to_name(current_station)
        directions.append(f"Start at {current_name} ({current_station})")
        
        for i in range(len(path_with_lines) - 1):
            current_station, lines = path_with_lines[i]
            next_station, _ = path_with_lines[i + 1]
            next_name = cls.complex_id_to_name(next_station)
            
            if lines:  # If there are lines to take
                line_str = ", ".join(lines)
                directions.append(f"Take the {line_str} trains to {next_name} ({next_station})")
            else:
                directions.append(f"Transfer at {next_name} ({next_station})")
                
        return "\n".join(directions)

    @classmethod
    def all_shortest_paths(cls, start: str, end: str) -> list[list[str]]:
        """Find all shortest paths between two stations.
        
        This method returns all possible paths that have the minimum number of
        transfers between two stations.
        
        Parameters:
            start (str): Complex ID of the starting station
            end (str): Complex ID of the destination station
            
        Returns:
            list[list[str]]: List of paths, where each path is a list of complex IDs
            
        Example:
            >>> SubwayGraph.all_shortest_paths("618", "164")
            [['618', '619', '164'], ['618', '620', '164']]
        """
        cls._assert_built()
        try:
            return list(nx.all_shortest_paths(cls.G, source=start, target=end))
        except nx.NetworkXNoPath:
            return []

    @classmethod
    def get_directions_for_path(cls, path: list[str]) -> list[dict]:
        """Get structured data for a specific path showing stations and available lines.
        
        Parameters:
            path (list[str]): List of complex IDs representing a path
            
        Returns:
            list[dict]: List of segments, where each segment is a dictionary with:
                - from_station: Complex ID of starting station
                - from_name: Name of starting station
                - to_station: Complex ID of destination station
                - to_name: Name of destination station
                - lines: List of subway lines that connect these stations
                
        Example:
            >>> path = ['618', '619', '164']
            >>> SubwayGraph.get_directions_for_path(path)
            [
                {
                    'from_station': '618',
                    'from_name': 'Times Square-42 St',
                    'to_station': '619',
                    'to_name': '5th Ave-53rd St',
                    'lines': ['N', 'Q', 'R', 'W']
                },
                {
                    'from_station': '619',
                    'from_name': '5th Ave-53rd St',
                    'to_station': '164',
                    'to_name': '14th St-Union Square',
                    'lines': ['E', 'M']
                }
            ]
        """
        segments = []
        for i in range(len(path) - 1):
            from_station = path[i]
            to_station = path[i + 1]
            from_name = cls.complex_id_to_name(from_station)
            to_name = cls.complex_id_to_name(to_station)
            lines = cls.connecting_lines(from_station, to_station)
            
            segments.append({
                'from_station': from_station,
                'from_name': from_name,
                'to_station': to_station,
                'to_name': to_name,
                'lines': lines
            })
            
        return segments

    @classmethod
    def get_all_directions(cls, start: str, end: str) -> list[list[dict]]:
        """Get structured data for all shortest paths between two stations.
        
        Parameters:
            start (str): Complex ID of the starting station
            end (str): Complex ID of the destination station
            
        Returns:
            list[list[dict]]: List of paths, where each path is a list of segments
                Each segment is a dictionary with:
                - from_station: Complex ID of starting station
                - from_name: Name of starting station
                - to_station: Complex ID of destination station
                - to_name: Name of destination station
                - lines: List of subway lines that connect these stations
                
        Example:
            >>> paths = SubwayGraph.get_all_directions("618", "164")
            >>> for path in paths:
            >>>     for segment in path:
            >>>         print(f"From {segment['from_name']} to {segment['to_name']}")
            >>>         print(f"Take lines: {', '.join(segment['lines'])}")
        """
        paths = cls.all_shortest_paths(start, end)
        return [cls.get_directions_for_path(path) for path in paths]


# ---------------------------------------------------------------------- #
# Simple smoke-test when run directly
# ---------------------------------------------------------------------- #
if __name__ == "__main__":
    SubwayGraph.build_graph()                # change path if needed
    print("Graph built with", len(SubwayGraph.G), "stations")
    print("Successors of 618:", SubwayGraph.successors("618"))  # Example complex ID
    print("Lines between 618 and 327:", SubwayGraph.connecting_lines("618", "327"))
