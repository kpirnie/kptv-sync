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
                else:
                    _data = retriever.get_json(endpoint)

            except Exception as e:
                print(f"Request failed: {e}")

        return _data

    # parse the m3u
    def _parse_m3u(self, m3u_content: str, provider: Dict[str, Any]) -> Optional[Dict[str, Dict[str, Any]]]:
        if not m3u_content:
            return None

        normalized = {}
        lines = m3u_content.splitlines()
        
        # Precompile regex patterns
        extinf_pattern = re.compile(r'#EXTINF:-1\s*(.*?),(.*)')  # More flexible spacing
        group_title_pattern = re.compile(r'group-title="([^"]*)"')
        tvg_id_pattern = re.compile(r'tvg-id="([^"]*)"')
        tvg_logo_pattern = re.compile(r'tvg-logo="([^"]*)"')
        adult_pattern = re.compile(r'adult="([^"]*)"')

        current_stream = None
        
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
                    current_stream['stream_icon'] = logo_match.group(1) if logo_match else provider.get('default_icon', "https://cdn.kevp.us/kp/kevinpirnie-favicon-initials.svg")
                    
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
                    "stream_icon": current_stream.get('stream_icon', provider.get('default_icon', "https://cdn.kevp.us/tv/kptv-icon.png"))
                }
                
                # Update stream_group based on stream_type
                if normalized[stream_id]['stream_type'] == 5:
                    normalized[stream_id]['stream_group'] = 'series'
                elif normalized[stream_id]['stream_type'] == 3:
                    normalized[stream_id]['stream_group'] = 'vod'
                
                # Reset for next entry
                current_stream = None
        
        return normalized or None

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
                continue
        
        # return the normalized data or nothing
        return normalized or None

    # get the streams
    def get_streams(self, provider):

        # setup the combined data
        combined = {}
        
        # Check if provider uses M3U (sp_type == 1)
        if provider.get('sp_type') == 1:
            self._enforce_request_delay()
            try:
                # Fetch the M3U content directly from sp_domain
                m3u_url = provider['sp_domain']
                m3u_content = self._safe_fetch(m3u_url, is_m3u=True)
                m3u_data = self._parse_m3u(m3u_content, provider)
                if m3u_data:
                    combined.update(m3u_data)
            except Exception as e:
                print(f"Failed to fetch M3U: {e}")
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
                
                # Handle any exceptions during fetching
                except Exception as e:
                    print(f"Failed to fetch {stream_type} streams ({endpoint}): {e}")
        
        return combined
    