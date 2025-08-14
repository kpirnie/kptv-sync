#!/usr/bin/env python3

# our necessary imports
from common.common import KP_Common
from utils.request import KP_Request
import time, sys, re, urllib.parse
from typing import Optional, Dict, Any, List, Union

# our retriever class
class KP_Get:

    # initialize the class   
    def __init__( self ):

        self.common = KP_Common( )
        self.last_request_time = 0
        self.min_request_interval = 1  # Conservative default delay (seconds)

    # enforce request delay
    def _enforce_request_delay( self ):
        
        # if the last request was less than the minimum interval, sleep
        elapsed = time.time( ) - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep( self.min_request_interval - elapsed )

        # set the last request time
        self.last_request_time = time.time( )

    # safe fetching of the streams
    def _safe_fetch( self, endpoint: str, is_m3u: bool = False ) -> Union[Dict[str, Any], str]:
        """
        Fetch data from endpoint, handling both JSON and M3U formats
        """
        
        headers = {
            "User-Agent": "VLC/3.0.21 LibVLC/3.0.21",
            "Accept": "application/json" if not is_m3u else "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }

        try:
            # Using context manager for automatic cleanup and error handling
            with KP_Request( default_headers=headers ) as retriever:
                if is_m3u:
                    if self.common.args.debug:
                        self.common.kp_print("info", f"Fetching M3U from: {endpoint}")
                    
                    _data = retriever.get_text(endpoint)
                    
                    if self.common.args.debug:
                        self.common.kp_print("info", f"M3U content length: {len(_data) if _data else 0}")
                        if _data:
                            # Show first few lines for debugging
                            lines = _data.split('\n')[:5]
                            self.common.kp_print("info", f"First lines: {lines}")
                    
                    return _data
                else:
                    if self.common.args.debug:
                        self.common.kp_print("info", f"Fetching JSON from: {endpoint}")
                    
                    _data = retriever.get_json(endpoint)
                    
                    if self.common.args.debug:
                        self.common.kp_print("info", f"JSON response type: {type(_data)}")
                    
                    return _data
        except Exception as e:
            if self.common.args.debug:
                self.common.kp_print("error", f"Request failed for {endpoint}: {e}")
            if is_m3u:
                return ""
            else:
                return {}

    # parse the m3u
    def _parse_m3u(self, m3u_content: str, provider: Dict[str, Any]) -> Optional[Dict[str, Dict[str, Any]]]:
        if not m3u_content or not m3u_content.strip():
            if self.common.args.debug:
                self.common.kp_print("warn", "M3U content is empty")
            return None

        if self.common.args.debug:
            self.common.kp_print("info", f"Parsing M3U content ({len(m3u_content)} chars)")
            
            # Show first few lines for debugging
            lines = m3u_content.splitlines()
            self.common.kp_print("info", "First 10 lines of M3U:")
            for i, line in enumerate(lines[:10]):
                self.common.kp_print("info", f"  {i+1}: {line}")
            self.common.kp_print("info", "---")
        else:
            lines = m3u_content.splitlines()

        normalized = {}
        processed_count = 0
        skipped_count = 0
        i = 0
        
        while i < len(lines):
            line = lines[i].strip()
            i += 1
            
            if not line:  # Skip empty lines
                continue
                
            if line.startswith('#EXTINF:-1'):
                if self.common.args.debug:
                    self.common.kp_print("info", f"Processing EXTINF line {i}: {line}")
                
                # Start collecting EXTINF data - may span multiple lines
                extinf_data = line
                
                # Look ahead to collect multi-line EXTINF data
                url_found = False
                while i < len(lines):
                    next_line = lines[i].strip()
                    if not next_line:
                        i += 1
                        continue
                    
                    if self.common.args.debug:
                        self.common.kp_print("info", f"  Checking next line {i+1}: {next_line}")
                    
                    # If we hit a URL line, we found our complete entry
                    if next_line.startswith('http'):
                        if self.common.args.debug:
                            self.common.kp_print("info", f"  Found URL: {next_line}")
                        
                        # Parse the EXTINF data we collected
                        stream_data = self._parse_extinf_line(extinf_data, provider, processed_count)
                        if stream_data:
                            if self.common.args.debug:
                                self.common.kp_print("info", f"  Successfully parsed stream: {stream_data.get('name', 'Unknown')}")
                            
                            stream_data['stream_url'] = next_line
                            
                            # Apply URL-based stream type detection
                            if any(x in next_line for x in ['/series/', '/shows/', '/show/']):
                                stream_data['stream_type'] = 5
                            elif any(x in next_line for x in ['/movie/', '/movies/', '/vod/']):
                                stream_data['stream_type'] = 3

                            # Generate stream_id safely
                            stream_name = stream_data.get('name', f"Stream_{processed_count}")
                            stream_id = re.sub(r'[^a-zA-Z0-9]', '', stream_name).lower()
                            
                            # Handle empty stream_id
                            if not stream_id:
                                stream_id = f"stream_{processed_count}"
                            
                            # Ensure unique stream_id
                            original_id = stream_id
                            counter = 1
                            while stream_id in normalized:
                                stream_id = f"{original_id}_{counter}"
                                counter += 1
                            
                            if self.common.args.debug:
                                self.common.kp_print("info", f"  Adding stream with ID: {stream_id}")
                            
                            # Add to normalized data
                            normalized[stream_id] = {
                                "stream_id": stream_id,
                                "stream_name": stream_data.get('name', stream_name),
                                "stream_url": stream_data.get('stream_url', ''),
                                "cat_id": stream_data.get('category_id', 'Uncategorized'),
                                "epg_id": stream_data.get('epg_channel_id', stream_name),
                                "is_adult": stream_data.get('is_adult', False),
                                "stream_type": stream_data.get('stream_type', 0),
                                "stream_group": "live",
                                "stream_icon": stream_data.get('stream_icon', provider.get('default_icon', "https://cdn.kevp.us/kp/kevinpirnie-favicon-initials.svg"))
                            }
                            
                            # Update stream_group based on stream_type
                            if normalized[stream_id]['stream_type'] == 5:
                                normalized[stream_id]['stream_group'] = 'series'
                            elif normalized[stream_id]['stream_type'] == 3:
                                normalized[stream_id]['stream_group'] = 'vod'
                            
                            processed_count += 1
                        else:
                            if self.common.args.debug:
                                self.common.kp_print("warn", f"  Failed to parse EXTINF data: {extinf_data}")
                            skipped_count += 1
                            
                        url_found = True
                        i += 1  # Move past the URL line
                        break
                        
                    # If we hit another #EXTINF, this entry has no URL - skip it
                    elif next_line.startswith('#EXTINF'):
                        if self.common.args.debug:
                            self.common.kp_print("warn", f"  Skipping EXTINF entry with no URL: {extinf_data[:100]}...")
                        skipped_count += 1
                        break
                        
                    # If we hit another # line that's not EXTINF, skip
                    elif next_line.startswith('#'):
                        if self.common.args.debug:
                            self.common.kp_print("warn", f"  Skipping EXTINF entry with no URL (hit other # line): {extinf_data[:100]}...")
                        skipped_count += 1
                        break
                        
                    # Otherwise, this line is part of the EXTINF data
                    else:
                        if self.common.args.debug:
                            self.common.kp_print("info", f"  Appending to EXTINF data: {next_line}")
                        extinf_data += " " + next_line
                        i += 1
                
                # If we reached end of file without finding URL
                if not url_found and i >= len(lines):
                    if self.common.args.debug:
                        self.common.kp_print("warn", f"  Skipping incomplete EXTINF entry: {extinf_data[:100]}...")
                    skipped_count += 1
        
        if self.common.args.debug:
            self.common.kp_print("info", "=== PARSING COMPLETE ===")
            self.common.kp_print("info", f"Parsed {processed_count} streams from M3U")
            if skipped_count > 0:
                self.common.kp_print("warn", f"Skipped {skipped_count} invalid/incomplete streams")
            self.common.kp_print("info", f"Total streams in normalized dict: {len(normalized)}")
        
        return normalized if normalized else None

    def _parse_extinf_line(self, extinf_data: str, provider: Dict[str, Any], line_num: int) -> Optional[Dict[str, Any]]:
        """
        Parse a complete EXTINF line that may span multiple lines
        """
        try:
            # Remove the #EXTINF:-1 prefix
            if not extinf_data.startswith('#EXTINF:-1'):
                return None
                
            extinf_data = extinf_data[11:].strip()  # Remove '#EXTINF:-1 '
            
            # Parse attributes with regex patterns first
            group_title_pattern = re.compile(r'group-title="([^"]*)"')
            tvg_id_pattern = re.compile(r'tvg-id="([^"]*)"')
            tvg_name_pattern = re.compile(r'tvg-name="([^"]*)"')
            tvg_logo_pattern = re.compile(r'tvg-logo="([^"]*)"')
            adult_pattern = re.compile(r'adult="([^"]*)"')
            
            # Find the last comma which separates attributes from the channel name
            last_comma = extinf_data.rfind(',')
            
            # Initialize with defaults
            name = f"Stream_{line_num}"
            attrs = extinf_data
            
            # If we found a comma, try to split attributes from name
            if last_comma != -1:
                attrs = extinf_data[:last_comma]
                potential_name = extinf_data[last_comma + 1:].strip()
                
                # Only use the comma-separated name if it's not empty
                if potential_name:
                    name = potential_name.strip('"').strip()
            
            # If we still don't have a good name, try to get it from tvg-name attribute
            if not name or name == f"Stream_{line_num}":
                tvg_name_match = tvg_name_pattern.search(attrs)
                if tvg_name_match and tvg_name_match.group(1):
                    name = tvg_name_match.group(1)
            
            # Build stream data with safe defaults
            stream_data = {
                'name': name,
                'category_id': 'Uncategorized',
                'epg_channel_id': name,
                'stream_icon': provider.get('default_icon', "https://cdn.kevp.us/kp/kevinpirnie-favicon-initials.svg"),
                'is_adult': False,
                'stream_type': provider.get('sp_stream_type', 0)
            }
            
            # Parse group-title
            group_match = group_title_pattern.search(attrs)
            if group_match and group_match.group(1):
                stream_data['category_id'] = group_match.group(1)
            
            # Parse tvg-id for EPG
            tvg_id_match = tvg_id_pattern.search(attrs)
            if tvg_id_match and tvg_id_match.group(1):
                stream_data['epg_channel_id'] = tvg_id_match.group(1)
            
            # Parse tvg-logo (handle truncated URLs gracefully)
            logo_match = tvg_logo_pattern.search(attrs)
            if logo_match and logo_match.group(1):
                logo_url = logo_match.group(1)
                # Only use the logo if it looks like a complete URL
                if logo_url.startswith('http') and not logo_url.endswith('>'):
                    stream_data['stream_icon'] = logo_url
            
            # Parse adult flag
            adult_match = adult_pattern.search(attrs)
            if adult_match:
                stream_data['is_adult'] = adult_match.group(1).lower() == 'true'
            
            # Determine stream type based on name and group
            name_lower = name.lower()
            group_lower = stream_data['category_id'].lower()
            
            if any(x in name_lower for x in ['24/7', 'series', 'shows', 'show']) or any(x in group_lower for x in ['series', 'shows']):
                stream_data['stream_type'] = 5
            elif any(x in name_lower for x in ['movie', 'movies', 'vod']) or any(x in group_lower for x in ['movies', 'vod']):
                stream_data['stream_type'] = 3
            
            return stream_data
            
        except Exception as e:
            if self.common.args.debug:
                self.common.kp_print("error", f"Error parsing EXTINF line: {e}")
            return None

    # normalize the data
    def _normalize_data( self, data: Union[List[Dict[str, Any]], str], data_type: str, provider: Dict[str, Any], ) -> Optional[Dict[str, Dict[str, Any]]]:
        
        # Handle M3U data passed as string
        if isinstance(data, str):
            return self._parse_m3u(data, provider)
            
        # if there is no data, just return nothing
        if not data:
            return None

        # Precompute URL template and extension
        url_template = getattr( self.common, f"stream_{data_type}", self.common.stream_live )
        ext = "ts" if provider.get("sp_stream_type", 0) == 0 else "m3u8"

        # if it's a vod stream
        if data_type == "vod":
            ext = provider.get( "container_extension", ext )

        # hold the returnable data
        normalized = {}

        # setup our 24/7 regex pattern
        _series_re_pattern = re.compile( r"24\/7|247|\/series\/|\/shows\/|\/show\/", re.IGNORECASE )

        # setup the VOD regex patterns
        _vod_re_pattern = re.compile( r"\/vods\/|\/vod\/|\/movies\/|\/movie\/", re.IGNORECASE )

        # loop over each item
        for item in data:
            
            # try to catch any errors
            try:
            
                # Extract common fields safely with .get()
                stream_id = item.get( "stream_id" ) or item.get( "series_id" )
            
                # if there's no stream id, this is invalid
                if not stream_id:
                    continue  # Skip invalid entries

                # Use match-case for type-specific logic (Python 3.10+)
                match data_type:

                    # live streams
                    case "live":

                        # setup the stream name to check
                        stream_name = item.get( "name", "").lower( )
                        stream_type = provider.get('sp_stream_type', 0)  # Use provider's stream type
                                                
                        # Check for series pattern match
                        if _series_re_pattern.search( stream_name ):
                            stream_type = 5

                        # Check for VOD pattern match
                        elif _vod_re_pattern.search( stream_name ):
                            stream_type = 3

                        # setup the stream data
                        stream_data = {
                            "cat_id": item["category_id"],
                            "epg_id": item["epg_channel_id"],
                            "is_adult": bool( item.get( "is_adult", 0 ) ),
                            "stream_type": stream_type,
                            "stream_group": "live",
                            "stream_icon": item.get( "stream_icon", "https://cdn.kevp.us/kp/kevinpirnie-favicon-initials.svg" ),
                        }

                    # series streams
                    case "series":
                        stream_data = {
                            "cat_id": item["category_id"],
                            "epg_id": item.get( "tmdb", item["name"] ),  # Default for missing TMDB
                            "is_adult": False,
                            "stream_type": 5,
                            "stream_group": "series",
                            "stream_icon": item.get( "cover", "https://cdn.kevp.us/kp/kevinpirnie-favicon-initials.svg" ),
                        }

                    # vod streams
                    case "vod":
                        stream_data = {
                            "cat_id": item["category_id"],
                            "epg_id": item.get( "tmdb", item["name"] ),
                            "is_adult": bool( item["is_adult"] ),
                            "stream_type": 3,
                            "stream_group": "vod",
                            "stream_icon": item.get( "stream_icon", "https://cdn.kevp.us/kp/kevinpirnie-favicon-initials.svg" ),
                        }

                    # invalid/unknown
                    case _:
                        continue  # Unknown type

                # Add universal fields
                stream_data.update( {
                    "stream_id": stream_id,
                    "stream_name": item["name"],
                    "stream_url": url_template % (
                        provider["sp_domain"],
                        provider["sp_username"],
                        provider["sp_password"],
                        stream_id,
                        ext
                    ) if provider.get('sp_stream_type', 0) != 1 else item.get('stream_url', ''),
                } )
                
                # setup the item
                normalized[stream_id] = stream_data

            # trap an error
            except (KeyError, TypeError) as e:
                # Optional: Log the error for debugging
                if self.common.args.debug:
                    self.common.kp_print("warn", f"Error processing item: {e}")
                continue
        
        # return the normalized data or nothing
        return normalized or None

    # get the streams
    def get_streams(self, provider):

        # setup the combined data
        combined = {}
        
        # Check if provider uses M3U (sp_type == 1)
        if provider.get('sp_type') == 1:
            if self.common.args.debug:
                self.common.kp_print("info", f"Processing M3U provider: {provider.get('sp_name', 'Unknown')}")
            
            self._enforce_request_delay()
            try:
                # Fetch the M3U content directly from sp_domain
                m3u_url = provider.get('sp_domain')
                
                if not m3u_url:
                    self.common.kp_print("error", f"No M3U URL found in provider {provider.get('sp_name', 'Unknown')}")
                    return combined
                
                if self.common.args.debug:
                    self.common.kp_print("info", f"M3U URL: {m3u_url}")
                
                m3u_content = self._safe_fetch(m3u_url, is_m3u=True)
                
                if not m3u_content:
                    self.common.kp_print("warn", f"No M3U content received from {m3u_url}")
                    return combined
                
                m3u_data = self._parse_m3u(m3u_content, provider)
                
                if m3u_data:
                    combined.update(m3u_data)
                    if self.common.args.debug:
                        self.common.kp_print("success", f"Successfully parsed {len(m3u_data)} streams from M3U")
                        self.common.kp_print("info", f"Combined dict now has: {len(combined)} streams")
                        
                        # Show first few stream keys for verification
                        stream_keys = list(combined.keys())[:5]
                        self.common.kp_print("info", f"Sample stream IDs: {stream_keys}")
                else:
                    self.common.kp_print("warn", "M3U parsing returned no data")
                    
            except KeyError as e:
                self.common.kp_print("error", f"Missing required field in M3U provider data: {e}")
                if self.common.args.debug:
                    self.common.kp_print("info", f"Provider data: {provider}")
            except Exception as e:
                self.common.kp_print("error", f"Failed to fetch M3U from {provider.get('sp_name', 'Unknown')}: {e}")
                if self.common.args.debug:
                    import traceback
                    traceback.print_exc()
        else:

            # Handle regular API endpoints
            for stream_type in ['live', 'series']:
                #for stream_type in ['live', 'series', 'vod']:

                # if we are only fetching live streams
                if self.common.args.live and stream_type != 'live':
                    continue

                # if we are only fetching series streams
                if self.common.args.series and stream_type != 'series':
                    continue

                # if we are only fetching vod streams
                if self.common.args.vod and stream_type != 'vod':
                    continue
                
                # Enforce request delay
                self._enforce_request_delay( )
                
                # Construct the API endpoint URL properly
                endpoint = getattr(self.common, f"api_{stream_type}") % (
                    provider['sp_domain'],
                    provider['sp_username'],
                    provider['sp_password']
                )

                # try to Fetch and normalize the data            
                try:
                    data = self._normalize_data(
                        self._safe_fetch(endpoint), 
                        stream_type, 
                        provider
                    )
                    if data:
                        combined.update(data)
                        if self.common.args.debug:
                            self.common.kp_print("info", f"Fetched {len(data)} {stream_type} streams")
                
                # Handle any exceptions during fetching
                except Exception as e:
                    self.common.kp_print("error", f"Failed to fetch {stream_type} streams from {provider.get('sp_name', 'Unknown')} ({endpoint}): {e}")
                    if self.common.args.debug:
                        import traceback
                        traceback.print_exc()
        
        if self.common.args.debug:
            self.common.kp_print("info", f"Returning {len(combined)} total streams to sync process")
        
        return combined