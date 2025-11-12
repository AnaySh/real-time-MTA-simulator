"""Utilities for fetching ridership data and estimating on/off-boardings.

This module centralises helper functions that were previously living inside
notebooks so they can be imported from regular Python code.  It relies on the
`SubwayGraph` and `ComplexesData` helpers to reason about the subway network
and uses the Socrata ridership dataset for demand figures.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Sequence, Union

import pandas as pd

from src.complexes import ComplexesData
from src.mta_graph import SubwayGraph
from src.socrata_od_client import get_ridership_data

ComplexId = Union[str, int]
Direction = Literal[0, 1]

_DEFAULT_COMPLEXES: ComplexesData | None = None


def _ensure_complexes(complexes: ComplexesData | None) -> ComplexesData:
    if complexes is not None:
        return complexes
    global _DEFAULT_COMPLEXES
    if _DEFAULT_COMPLEXES is None:
        _DEFAULT_COMPLEXES = ComplexesData()
    return _DEFAULT_COMPLEXES


def _to_int(value: ComplexId, *, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Unable to interpret {field} value {value!r} as an integer") from exc


def _target_stations(sequence: Sequence[int], target: int) -> list[int]:
    try:
        index = sequence.index(target)
    except ValueError:
        return []
    return list(sequence[index + 1 :])


def init(
    *,
    build_graph: bool = True,
    complexes: ComplexesData | None = None,
    gtfs_dir: str | Path | None = None,
) -> None:
    """Initialise shared state used by boarding/off-boarding helpers.

    Parameters
    ----------
    build_graph
        When True (default) ensures `SubwayGraph` has built its GTFS-backed
        graph. Pass False if the caller has already done so.
    complexes
        Optional pre-built `ComplexesData` instance to reuse across calls.
        When omitted, a singleton instance is created on demand.
    gtfs_dir
        Optional override for the GTFS directory passed to
        `SubwayGraph.build_graph`. Only used when `build_graph` is True.
    """

    global _DEFAULT_COMPLEXES
    if complexes is not None:
        _DEFAULT_COMPLEXES = complexes

    if build_graph:
        if gtfs_dir is not None:
            SubwayGraph.build_graph(gtfs_dir)
        else:
            SubwayGraph.build_graph()

    if _DEFAULT_COMPLEXES is None:
        _DEFAULT_COMPLEXES = ComplexesData()


def get_ordered_stops(
    line: str,
    direction: Direction,
    return_type: Literal[0, 1, 2] = 0,
    complexes: ComplexesData | None = None,
) -> Union[dict[str, int], list[int], list[str]]:
    """Return ordered stops for a subway line and direction.

    Parameters
    ----------
    line
        Subway line identifier (e.g. ``"L"`` or ``"A"``).
    direction
        ``0`` for north/east-bound, ``1`` for south/west-bound. Must match the
        semantics used by `SubwayGraph.ordered_stops`.
    return_type
        ``0`` (default) returns a mapping of station name -> complex id,
        ``1`` returns a list of complex ids, ``2`` returns a list of station
        names in order.
    complexes
        Optional pre-instantiated `ComplexesData` to avoid reloading data on
        repeated calls.

    Notes
    -----
    `SubwayGraph.build_graph()` must be called before this helper so that the
    GTFS data is loaded.
    """

    complexes_data = _ensure_complexes(complexes)
    gtfs_stops = SubwayGraph.ordered_stops(line, direction)

    complex_ids: list[int] = []
    complex_names: list[str] = []

    for stop_id in gtfs_stops:
        complex_id = complexes_data.get_complex_id_by_gtfs_stop_id(stop_id)
        if complex_id is None:
            continue
        complex_ids.append(int(complex_id))
        name = complexes_data.get_station_name_by_gtfs_id(stop_id)
        complex_names.append(name if name is not None else str(complex_id))

    if return_type == 0:
        return dict(zip(complex_names, complex_ids))
    if return_type == 1:
        return complex_ids
    if return_type == 2:
        return complex_names
    raise ValueError("return_type must be 0 (dict), 1 (complex ids) or 2 (names)")


def get_onboards_data(
    complex_id: ComplexId,
    year: int | None = None,
    month: int | None = None,
    day_of_week: str | None = None,
    hour_of_day: int | None = None,
    app_token: str | None = None,
) -> pd.DataFrame:
    """Fetch ridership data for passengers boarding at a complex."""

    df = get_ridership_data(
        year=year,
        month=month,
        day_of_week=day_of_week,
        hour_of_day=hour_of_day,
        origin_station_complex_id=complex_id,
        app_token=app_token,
    )
    if df.empty:
        return df
    return df.sort_values("estimated_average_ridership", ascending=False).reset_index(drop=True)


def get_offboards_data(
    complex_id: ComplexId,
    year: int | None = None,
    month: int | None = None,
    day_of_week: str | None = None,
    hour_of_day: int | None = None,
    app_token: str | None = None,
) -> pd.DataFrame:
    """Fetch ridership data for passengers alighting at a complex."""

    df = get_ridership_data(
        year=year,
        month=month,
        day_of_week=day_of_week,
        hour_of_day=hour_of_day,
        destination_station_complex_id=complex_id,
        app_token=app_token,
    )
    if df.empty:
        return df
    return df.sort_values("estimated_average_ridership", ascending=False).reset_index(drop=True)


def get_onboardings(
    origin_complex_id: ComplexId,
    line: str,
    direction: Direction,
    ridership_df: pd.DataFrame,
    complexes: ComplexesData | None = None,
    verbose: bool = False,
) -> tuple[float, pd.DataFrame]:
    """Estimate on-boardings for a station on a given line and direction."""

    complexes_data = _ensure_complexes(complexes)
    origin_int = _to_int(origin_complex_id, field="origin_complex_id")
    origin_str = str(origin_int)

    stops_on_line = get_ordered_stops(line, direction, 1, complexes_data)
    if origin_int not in stops_on_line:
        raise ValueError(f"Origin complex id {origin_complex_id!r} is not on line {line} (direction {direction})")

    stops_after = _target_stations(stops_on_line, origin_int)
    stops_after_set = set(stops_after)

    total_boardings = 0.0
    result_rows: list[dict] = []

    for _, row in ridership_df.iterrows():
        dest_raw = row.get("destination_station_complex_id")
        try:
            dest_int = _to_int(dest_raw, field="destination_station_complex_id")
        except ValueError:
            if verbose:
                print(f"Skipping destination {dest_raw!r}: cannot convert to int")
            continue
        dest_str = str(dest_int)

        connections = SubwayGraph.connecting_lines(origin_str, dest_str)
        if verbose:
            print(f"From {origin_str} to {dest_str}, connections: {connections}")

        contribution = 0.0

        if connections and line in connections and dest_int in stops_after_set:
            weight = 1.0 / len(connections)
            contribution = row["estimated_average_ridership"] * weight
            if verbose:
                print(f"Direct path via {line}: weight={weight:.3f}, contribution={contribution:.3f}")
        else:
            shortest_paths = SubwayGraph.all_shortest_paths(origin_str, dest_str)
            
            # Filter out unrealistically long paths (outliers)
            if shortest_paths:
                # Calculate distance for each path using get_directions_for_path
                path_distances = []
                for path in shortest_paths:
                    _, total_distance = SubwayGraph.get_directions_for_path(path)
                    path_distances.append((path, total_distance))
                
                # Filter: keep only paths <= 2x the shortest path distance
                min_distance = min(d for _, d in path_distances)
                threshold = min_distance * 2.0
                shortest_paths = [path for path, d in path_distances if d <= threshold]
                
                if verbose:
                    original_count = len(path_distances)
                    filtered_count = len(shortest_paths)
                    if original_count != filtered_count:
                        print(f"Filtered paths: {original_count} -> {filtered_count} (removed {original_count - filtered_count} outliers)")

            total_paths = 0
            num_paths = 0
            for path in shortest_paths:
                if len(path) < 2:
                    continue
                first_leg = path[:2]
                leg_connections = SubwayGraph.connecting_lines(first_leg[0], first_leg[1])
                if not leg_connections:
                    continue
                total_paths += len(leg_connections)
                if line in leg_connections:
                    try:
                        next_stop_int = _to_int(first_leg[1], field="path station")
                    except ValueError:
                        continue
                    if next_stop_int in stops_after_set:
                        num_paths += 1

            if total_paths and num_paths:
                weight = num_paths / total_paths
                contribution = row["estimated_average_ridership"] * weight
                if verbose:
                    print(
                        f"Transfer path via {line}: total_paths={total_paths}, "
                        f"num_paths={num_paths}, weight={weight:.3f}, contribution={contribution:.3f}"
                    )
            elif verbose and shortest_paths:
                print("No qualifying transfer paths found for this destination.")

        if contribution > 0:
            total_boardings += contribution
            row_dict = row.to_dict()
            row_dict["estimated_average_ridership"] = contribution
            result_rows.append(row_dict)

    boardings_df = pd.DataFrame(result_rows, columns=ridership_df.columns)
    return total_boardings, boardings_df


def get_offboardings(
    destination_complex_id: ComplexId,
    line: str,
    direction: Direction,
    ridership_df: pd.DataFrame,
    complexes: ComplexesData | None = None,
    verbose: bool = False,
) -> tuple[float, pd.DataFrame]:
    """Estimate alightings for a station on a given line and direction."""

    complexes_data = _ensure_complexes(complexes)
    destination_int = _to_int(destination_complex_id, field="destination_complex_id")
    destination_str = str(destination_int)

    forward_stops = get_ordered_stops(line, direction, 1, complexes_data)
    if destination_int not in forward_stops:
        raise ValueError(
            f"Destination complex id {destination_complex_id!r} is not on line {line} (direction {direction})"
        )

    reverse_stops = get_ordered_stops(line, 1 - direction, 1, complexes_data)
    stops_before = _target_stations(reverse_stops, destination_int)
    stops_before_set = set(stops_before)

    total_offboardings = 0.0
    result_rows: list[dict] = []

    for _, row in ridership_df.iterrows():
        origin_raw = row.get("origin_station_complex_id")
        try:
            origin_int = _to_int(origin_raw, field="origin_station_complex_id")
        except ValueError:
            if verbose:
                print(f"Skipping origin {origin_raw!r}: cannot convert to int")
            continue
        origin_str = str(origin_int)

        connections = SubwayGraph.connecting_lines(destination_str, origin_str)
        if verbose:
            print(f"From {origin_str} to {destination_str}, connections: {connections}")

        contribution = 0.0

        if connections and line in connections and origin_int in stops_before_set: # should check that train is not in the oppositte direction
            weight = 1.0 / len(connections)
            contribution = row["estimated_average_ridership"] * weight
            if verbose:
                print(f"Direct path via {line}: weight={weight:.3f}, contribution={contribution:.3f}")
        else:
            shortest_paths = SubwayGraph.all_shortest_paths(destination_str, origin_str)
            
            # Filter out unrealistically long paths (outliers)
            if shortest_paths:
                # Calculate distance for each path using get_directions_for_path
                path_distances = []
                for path in shortest_paths:
                    _, total_distance = SubwayGraph.get_directions_for_path(path)
                    path_distances.append((path, total_distance))
                
                # Filter: keep only paths <= 2x the shortest path distance
                min_distance = min(d for _, d in path_distances)
                threshold = min_distance * 2.0
                shortest_paths = [path for path, d in path_distances if d <= threshold]
                
                if verbose:
                    original_count = len(path_distances)
                    filtered_count = len(shortest_paths)
                    if original_count != filtered_count:
                        print(f"Filtered paths: {original_count} -> {filtered_count} (removed {original_count - filtered_count} outliers)")
            
            total_paths = 0
            num_paths = 0
            for path in shortest_paths:
                if len(path) < 2:
                    continue
                first_leg = path[:2]
                leg_connections = SubwayGraph.connecting_lines(first_leg[0], first_leg[1])
                if not leg_connections:
                    continue
                total_paths += len(leg_connections)
                if line in leg_connections:
                    try:
                        next_stop_int = _to_int(first_leg[1], field="path station")
                    except ValueError:
                        continue
                    if next_stop_int in stops_before_set:
                        num_paths += 1

            if total_paths and num_paths:
                weight = num_paths / total_paths
                contribution = row["estimated_average_ridership"] * weight
                if verbose:
                    print(
                        f"Transfer path via {line}: total_paths={total_paths}, "
                        f"num_paths={num_paths}, weight={weight:.3f}, contribution={contribution:.3f}"
                    )
            elif verbose and shortest_paths:
                print("No qualifying transfer paths found for this origin.")

        if contribution > 0:
            total_offboardings += contribution
            row_dict = row.to_dict()
            row_dict["estimated_average_ridership"] = contribution
            result_rows.append(row_dict)

    offboardings_df = pd.DataFrame(result_rows, columns=ridership_df.columns)
    return total_offboardings, offboardings_df


__all__ = [
    "init",
    "get_ordered_stops",
    "get_onboards_data",
    "get_onboardings",
    "get_offboards_data",
    "get_offboardings",
]

