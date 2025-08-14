#!/usr/bin/env python3

import regex
from typing import Dict, List, Any

# Import debug utilities
try:
    from utils.debug import debug_print_sync
except ImportError:
    def debug_print_sync(msg): pass

# our filter class
class KP_Filter:

    # setup the match pattern
    @staticmethod
    def _match_pattern( pattern: str, text: str ) -> bool:

        # try to match the pattern and string
        try:

            # Use regex.IGNORECASE flag directly in compile
            _pattern = regex.compile( pattern, regex.IGNORECASE )
            
            # Return True if ANY matches found (> 0 instead of > 1)
            result = bool( _pattern.findall( text ) )
            debug_print_sync(f"Pattern '{pattern}' {'matched' if result else 'did not match'} text '{text[:50]}{'...' if len(text) > 50 else ''}'")
            return result
        except Exception as e:
            debug_print_sync(f"Pattern matching error for '{pattern}': {e}")
            return False
        
    # filter the normalized streams
    @staticmethod
    def filter_streams( normalized_data: Dict[str, Dict[str, Any]], db_filters: List[Dict[str, Any]] ) -> Dict[str, Dict[str, Any]]:
        
        debug_print_sync(f"Starting stream filtering: {len(normalized_data)} streams, {len(db_filters)} filters")
        
        # if there aren't any filters
        if not db_filters:
            debug_print_sync("No filters found, returning all streams")
            return normalized_data
        
        # hold the returnable streams
        filtered_streams = {}
        
        # Count filter types for debugging
        include_filters = [f for f in db_filters if f["sf_type_id"] == 0]
        exclude_filters = [f for f in db_filters if f["sf_type_id"] != 0]
        
        debug_print_sync(f"Filter breakdown: {len(include_filters)} include filters, {len(exclude_filters)} exclude filters")
        
        included_count = 0
        excluded_count = 0
        processed_count = 0
        
        # loop the originating data
        for stream_id, stream in normalized_data.items( ):
            processed_count += 1

            # hold the name, url and group
            stream_name = stream["stream_name"]
            stream_url = stream["stream_url"]
            
            # Check includes first
            is_included = any(
                filter_rule["sf_type_id"] == 0 and 
                KP_Filter._match_pattern( filter_rule["sf_filter"], stream_name )
                for filter_rule in db_filters
            )
            
            # if it's supposed to be included, add the stream and skip the excludes
            if is_included:
                filtered_streams[stream_id] = stream
                included_count += 1
                debug_print_sync(f"Stream '{stream_name}' INCLUDED by include filter")
                continue

            # Default exclusion flag
            should_exclude = False

            # loop the filters again
            for filter_rule in db_filters:

                # setup the type and value of the filters
                filter_type = filter_rule["sf_type_id"]
                filter_value = filter_rule["sf_filter"]
                
                # if we just need to check if the name contains a string
                if filter_type == 1:

                    # check if the lowercase filter is contained in the stream name
                    if filter_value.lower( ) in stream_name.lower( ):
                        should_exclude = True
                        debug_print_sync(f"Stream '{stream_name}' EXCLUDED by contains filter: '{filter_value}'")
                        break

                # check the regex name or stream
                elif filter_type in (2, 3): 

                    # setup the target
                    target = stream_name if filter_type == 2 else stream_url

                    # search the name for the regular expression
                    if KP_Filter._match_pattern( filter_value, target ):
                        should_exclude = True
                        debug_print_sync(f"Stream '{stream_name}' EXCLUDED by regex filter: '{filter_value}'")
                        break
            
            # if the stream should NOT be included, add it to the return
            if not should_exclude:
                filtered_streams[stream_id] = stream
            else:
                excluded_count += 1
        
        debug_print_sync(f"Filtering completed: {processed_count} processed, {included_count} included, {excluded_count} excluded, {len(filtered_streams)} final")
        
        # return the filtered streams
        return filtered_streams