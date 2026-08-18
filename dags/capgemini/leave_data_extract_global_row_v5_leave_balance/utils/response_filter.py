import itertools
import rail


def prepare_export_items(region_country_mapper, max_batch_count):
    """Prepare individual export items with batch assignment using round-robin distribution."""
    export_items = []
    index = 0
    for region in region_country_mapper:
        for country_entry in region.get('countries', []):
            # Round-robin batch assignment: country 0 → batch 1, country 1 → batch 2, etc.
            batch_num = (index % max_batch_count) + 1

            export_items.append({
                "region": region['region'],
                "region_code": region['region_code'],
                "country_code": country_entry['country_code'],
                "country_list": country_entry['country_list'],
                "filename_format": country_entry['filename_format'],
                "batch_num": batch_num
            })
            index += 1

    return export_items


def build_location_hierarchy_map(all_responses, region_country_mapper):
    """Build a map of country name to all location URIs (including children).

    The API returns flat rows where:
    - hierarchyLevel 0 = country level
    - hierarchyLevel 1+ = child locations
    - cells[1].cellCollection contains the full path (first item is the root country)

    Returns artifact path containing the location map.
    """
    # Get all country names from mapper
    all_countries = []
    for region in region_country_mapper:
        for country_entry in region.get('countries', []):
            all_countries.extend(country_entry.get('country_list', []))
    country_names_lower = [c.lower() for c in set(all_countries)]

    # Flatten rows from all pages
    flattened_rows = list(itertools.chain(*list(map(lambda x: x['rows'], all_responses))))

    # Build location map: country -> list of all URIs (country + all children)
    location_map = {}

    for row in flattened_rows:
        cells = row.get('cells', [])
        if not cells:
            continue

        # Get the root country from cellCollection (first item in the path)
        cell_collection = cells[1].get('cellCollection', []) if len(cells) > 1 else []
        if not cell_collection:
            continue

        root_country_name = cell_collection[0].get('textValue', '')

        # Check if this root country matches a mapper country (case-insensitive)
        if root_country_name.lower() in country_names_lower:
            # Get the URI of this location
            location_uri = cells[0].get('uri')
            if location_uri:
                if root_country_name not in location_map:
                    location_map[root_country_name] = []
                location_map[root_country_name].append(location_uri)

    return rail.write_json_artifact(location_map)


def get_location_uris_for_country(dag_run):
    """Get location URIs for a specific country from the pre-built artifact."""
    country_list = dag_run.conf.get('country_list', [])
    artifact_path = dag_run.conf.get('location_hierarchy_artifact')
    location_hierarchy_map = rail.load_json_artifact(artifact_path)
    all_uris = []

    for country in country_list:
        for loc_name, uris in location_hierarchy_map.items():
            if loc_name.lower() == country.lower():
                all_uris.extend(uris)
                break

    return all_uris
