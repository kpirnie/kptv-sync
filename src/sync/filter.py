#!/usr/bin/env python3

import regex
from typing import Dict, List, Any

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
            
            # DEBUG: Print pattern matching details
            #print(f"  PATTERN DEBUG: '{pattern}' vs '{text}' = {result}")
            
            return result
        except Exception as e:
            print(f"  PATTERN ERROR: '{pattern}' vs '{text}' - {e}")
            return False
        
    # filter the normalized streams
    @staticmethod
    def filter_streams( normalized_data: Dict[str, Dict[str, Any]], db_filters: List[Dict[str, Any]] ) -> Dict[str, Dict[str, Any]]:
        
        # if there aren't any filters
        if not db_filters:
            #print("FILTER DEBUG: No filters found")
            return normalized_data
        
        # DEBUG: Show filter summary
        include_filters = [f for f in db_filters if f["sf_type_id"] == 0]
        exclude_filters = [f for f in db_filters if f["sf_type_id"] != 0]
        #print(f"FILTER DEBUG: {len(include_filters)} include filters, {len(exclude_filters)} exclude filters")
        
        #for i, inc_filter in enumerate(include_filters):
        #    print(f"  Include Filter {i+1}: '{inc_filter['sf_filter']}'")
        
        # hold the returnable streams
        filtered_streams = {}
        include_count = 0
        exclude_count = 0
        
        # loop the originating data
        for stream_id, stream in normalized_data.items( ):

            # hold the name, url and group
            stream_name = stream["stream_name"]
            stream_url = stream["stream_url"]
            
            # DEBUG: Show first few streams being processed
            #if len(filtered_streams) < 5:
            #    print(f"\nFILTER DEBUG: Processing stream '{stream_name}'")
            
            # Check includes first
            is_included = any(
                filter_rule["sf_type_id"] == 0 and 
                KP_Filter._match_pattern( filter_rule["sf_filter"], stream_name )
                for filter_rule in db_filters
            )
            
            # if it's supposed to be included, add the stream and skip the excludes
            if is_included:
                filtered_streams[stream_id] = stream
                include_count += 1
                #if include_count <= 5:  # Show first few included streams
                #    print(f"  INCLUDED: '{stream_name}'")
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
                        break

                # check the regex name or stream
                elif filter_type in (2, 3): 

                    # setup the target
                    target = stream_name if filter_type == 2 else stream_url

                    # search the name for the regular expression
                    if KP_Filter._match_pattern( filter_value, target ):
                        should_exclude = True
                        break
            
            # if the stream should NOT be included, add it to the return
            if not should_exclude:
                filtered_streams[stream_id] = stream
            else:
                exclude_count += 1
        
        print(f"\nFILTER SUMMARY:")
        print(f"  Input streams: {len(normalized_data)}")
        print(f"  Included by include filters: {include_count}")
        print(f"  Excluded by exclude filters: {exclude_count}")
        print(f"  Final output streams: {len(filtered_streams)}")
        
        # return the filtered streams
        return filtered_streams