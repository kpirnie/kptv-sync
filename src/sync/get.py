#!/usr/bin/env python3

# our necessary imports
from common.common import KP_Common
from utils.request import KP_Request
import time, sys, re, urllib.parse
from typing import Optional, Dict, Any, List, Union

# Import debug utilities
try:
    from utils.debug import debug_print_sync, debug_print_request
except ImportError:
    def debug_print_sync(msg): pass
    def debug_print_request(msg): pass

# our retriever class
class KP_Get:

    # initialize the class   
    def __init__( self ):

        self.common = KP_Common( )
        self.last_request_time = 0
        self.min_request_interval = 1  # Conservative default delay (seconds)
        
        debug_print_sync("KP_Get initialized")

    # enforce request delay
    def _enforce_request_delay( self ):
        
        # if the last request was less than the minimum interval, sleep
        elapsed = time.time( ) - self.last_request_time
        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            debug_print_request(f"Enforcing request delay: sleeping {sleep_time:.2f} seconds")
            time.sleep( sleep_time )

        # set the last request time
        self.last_request_time = time.time( )

    # safe fetching of the streams
    def _safe_fetch( self, endpoint: str, is_m3u: bool = False ) -> Union[Dict[str, Any], str]:
        """
        Fetch data from endpoint, handling both JSON and M3U formats
        """
        debug_print_request(f"Fetching {'M3U' if is_m3u else 'JSON'} from: {endpoint}")
        
        # setup the return data
        _data = {} if not is_m3u else ""

        headers = {
            "User-Agent": "VLC/3.0.21 LibVLC/3.0.21",
            "Accept": "application/json" if not is_m3u else "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }

        # Using context manager for automatic cleanup and error handling
        with KP_Request( default_headers=headers ) as retriever:
            try:
                if is_m3u:
                    _data = retriever.get_text(endpoint)
                    debug_print_request(f"Retrieved M3U content: {len(_data)} characters")
                else:
                    _data = retriever.get_json(endpoint)
                    debug_print_request(f"Retrieved JSON data: {len(_data) if isinstance(_data, list) else 'dict'} items")

            except Exception as e:
                debug_print_request(f"Request failed for {endpoint}: {e}")
                # Don't print error to console unless debug mode
                pass

        return _data

    # parse the m3u
    def _parse_m3u(self, m3u_content: str, provider: Dict[str, Any]) -> Optional[Dict[str, Dict[str, Any]]]:
        if not m3u_content:
            debug_print_sync("M3U content is empty")
            return None

        debug_print_sync(f"Parsing M3U content: {len(m3u_content)} characters")

        normalized = {}
        lines = m3u_content.splitlines()
        
        # Precompile regex patterns
        extinf_pattern = re.compile(r'#EXTINF:-1\s*(.*?),(.*)')  # More flexible spacing
        group_title_pattern = re.compile(r'group-title="([^"]*)"')
        tvg_id_pattern = re.compile(r'tvg-id="([^"]*)"')
        tvg_logo_pattern = re.compile(r'tvg-logo="([^"]*)"')
        adult_pattern = re.compile(r'adult="([^"]*)"')

        current_stream = None
        processed_streams = 0
        
        for line in lines:
            line = line.strip()
            if not line:  # Skip empty lines
                continue
                
            if line.startswith('#EXTINF:-1'):
                # New stream entry
                current_stream = {}
                
                # Parse EXTINF line
                match = extinf_pattern.match(line)
                if match:
                    attrs, name = match.groups()
                    current_stream['name'] = name.strip()
                    
                    # Parse attributes
                    group_match = group_title_pattern.search(attrs)
                    current_stream['category_id'] = group_match.group(1) if group_match else "Uncategorized"
                    
                    tvg_id_match = tvg_id_pattern.search(attrs)
                    current_stream['epg_channel_id'] = tvg_id_match.group(1) if tvg_id_match else current_stream['name']
                    
                    logo_match = tvg_logo_pattern.search(attrs)
                    current_stream['stream_icon'] = logo_match.group(1) if logo_match else provider.get('default_icon', "https://cdn.kevp.us/tv/kptv-icon.svg")
                    
                    adult_match = adult_pattern.search(attrs)
                    current_stream['is_adult'] = bool(adult_match and adult_match.group(1).lower() == 'true')
                    
                    # Determine stream type
                    current_stream['stream_type'] = provider.get('sp_stream_type', 0)
                    name_lower = current_stream['name'].lower()
                    if any(x in name_lower for x in ['24/7', 'series', 'shows', 'show']):
                        current_stream['stream_type'] = 5
                    elif any(x in name_lower for x in ['movie', 'movies', 'vod']):
                        current_stream['stream_type'] = 3
                        
            elif current_stream is not None and line.startswith('http'):
                # This is the stream URL for the current entry
                current_stream['stream_url'] = line

                if any(x in line for x in ['/series/', '/shows/', '/show/']):
                    current_stream['stream_type'] = 5
                elif any(x in line for x in ['/movie/', '/movies/', '/vod/']):
                    current_stream['stream_type'] = 3

                # Generate stream_id
                stream_id = re.sub(r'[^a-zA-Z0-9]', '', current_stream['name']).lower()
                
                # Add to normalized data
                normalized[stream_id] = {
                    "stream_id": stream_id,
                    "stream_name": current_stream['name'],
                    "stream_url": current_stream['stream_url'],
                    "cat_id": current_stream.get('category_id', 'Uncategorized'),
                    "epg_id": current_stream.get('epg_channel_id', current_stream['name']),
                    "is_adult": current_stream.get('is_adult', False),
                    "stream_type": current_stream.get('stream_type', 0),
                    "stream_group": "live",  # Default
                    "stream_icon": current_stream.get('stream_icon', provider.get('default_icon', "https://cdn.kevp.us/tv/kptv-icon.svg"))
                }
                
                # Update stream_group based on stream_type
                if normalized[stream_id]['stream_type'] == 5:
                    normalized[stream_id]['stream_group'] = 'series'
                elif normalized[stream_id]['stream_type'] == 3:
                    normalized[stream_id]['stream_group'] = 'vod'
                
                processed_streams += 1
                # Reset for next entry
                current_stream = None
        
        debug_print_sync(f"M3U parsing completed: {processed_streams} streams processed")
        return normalized or None

    # normalize the data
    def _normalize_data( self, data: Union[List[Dict[str, Any]], str], data_type: str, provider: Dict[str, Any], ) -> Optional[Dict[str, Dict[str, Any]]]:
        
        debug_print_sync(f"Normalizing {data_type} data")
        
        # Handle M3U data passed as string
        if isinstance(data, str):
            return self._parse_m3u(data, provider)
            
        # if there is no data, just return nothing
        if not data:
            debug_print_sync(f"No {data_type} data to normalize")
            return None

        debug_print_sync(f"Normalizing {len(data)} {data_type} items")

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

        processed_count = 0
        skipped_count = 0

        # loop over each item
        for item in data:
            
            # try to catch any errors
            try:
            
                # Extract common fields safely with .get()
                stream_id = item.get( "stream_id" ) or item.get( "series_id" )
            
                # if there's no stream id, this is invalid
                if not stream_id:
                    skipped_count += 1
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
                            "stream_icon": item.get( "stream_icon", "https://cdn.kevp.us/tv/kptv-icon.svg" ),
                        }

                    # series streams
                    case "series":
                        stream_data = {
                            "cat_id": item["category_id"],
                            "epg_id": item.get( "tmdb", item["name"] ),  # Default for missing TMDB
                            "is_adult": False,
                            "stream_type": 5,
                            "stream_group": "series",
                            "stream_icon": item.get( "cover", "https://cdn.kevp.us/tv/kptv-icon.svg" ),
                        }

                    # vod streams
                    case "vod":
                        stream_data = {
                            "cat_id": item["category_id"],
                            "epg_id": item.get( "tmdb", item["name"] ),
                            "is_adult": bool( item["is_adult"] ),
                            "stream_type": 3,
                            "stream_group": "vod",
                            "stream_icon": item.get( "stream_icon", "https://cdn.kevp.us/tv/kptv-icon.svg" ),
                        }

                    # invalid/unknown
                    case _:
                        skipped_count += 1
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
                processed_count += 1

            # trap an error
            except (KeyError, TypeError) as e:
                # Optional: Log the error for debugging
                debug_print_sync(f"Error processing item: {e}")
                skipped_count += 1
                continue
        
        debug_print_sync(f"Normalization completed: {processed_count} processed, {skipped_count} skipped")
        # return the normalized data or nothing
        return normalized or None

    # get the streams
    def get_streams(self, provider):

        debug_print_sync(f"Getting streams for provider: {provider['sp_name']}")

        # setup the combined data
        combined = {}
        
        # Check if provider uses M3U (sp_type == 1)
        if provider.get('sp_type') == 1:
            debug_print_sync("Provider uses M3U format")
            self._enforce_request_delay()
            try:
                # Fetch the M3U content directly from sp_domain
                m3u_url = provider['sp_domain']
                m3u_content = self._safe_fetch(m3u_url, is_m3u=True)
                m3u_data = self._parse_m3u(m3u_content, provider)
                if m3u_data:
                    combined.update(m3u_data)
                    debug_print_sync(f"M3U data processed: {len(m3u_data)} streams")
            except Exception as e:
                debug_print_sync(f"Failed to fetch M3U: {e}")
        else:
            debug_print_sync("Provider uses API format")

            # Handle regular API endpoints
            for stream_type in ['live', 'series']:
                #for stream_type in ['live', 'series', 'vod']:

                # if we are only fetching live streams
                if self.common.args.live and stream_type != 'live':
                    debug_print_sync(f"Skipping {stream_type} streams (--live flag set)")
                    continue

                # if we are only fetching series streams
                if self.common.args.series and stream_type != 'series':
                    debug_print_sync(f"Skipping {stream_type} streams (--series flag set)")
                    continue

                # if we are only fetching vod streams
                if self.common.args.vod and stream_type != 'vod':
                    debug_print_sync(f"Skipping {stream_type} streams (--vod flag set)")
                    continue
                
                debug_print_sync(f"Fetching {stream_type} streams")
                
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
                        debug_print_sync(f"{stream_type.title()} streams processed: {len(data)} items")
                
                # Handle any exceptions during fetching
                except Exception as e:
                    debug_print_sync(f"Failed to fetch {stream_type} streams ({endpoint}): {e}")
        
        debug_print_sync(f"Total streams retrieved: {len(combined)}")
        return combined