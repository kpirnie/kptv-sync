#!/usr/bin/env python3

import subprocess
import json
import time
import requests
import tempfile
import os
from typing import Dict, Any, Optional, Tuple

try:
    from utils.debug import debug_print_sync
except ImportError:
    def debug_print_sync(msg): pass

class KP_StreamTester:
    
    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        self.ffprobe_available = self._check_ffprobe_available()
        
    def _check_ffprobe_available(self) -> bool:
        try:
            result = subprocess.run(['ffprobe', '-version'], 
                                  capture_output=True, 
                                  text=True, 
                                  timeout=5)
            return result.returncode == 0
        except:
            return False
    
    def _test_with_http_then_ffprobe(self, stream_url: str) -> Tuple[bool, str]:
        """Test with HTTP first, then if valid, test with ffprobe using same connection"""
        debug_print_sync(f"Testing stream with HTTP+ffprobe: {stream_url[:50]}...")
        
        try:
            headers = {
                'User-Agent': 'VLC/3.0.21 LibVLC/3.0.21',
                'Accept': '*/*',
                'Connection': 'keep-alive'
            }
            
            # Start the request but don't read the full content yet
            with requests.get(stream_url, 
                            headers=headers, 
                            timeout=self.timeout, 
                            stream=True,
                            allow_redirects=True) as response:
                
                # First check HTTP status
                if response.status_code not in [200, 206]:
                    return False, f"HTTP error: {response.status_code}"
                
                # Get content type and URL info for analysis
                content_type = response.headers.get('content-type', '').lower()
                url_lower = stream_url.lower()
                
                # Read a chunk to verify we can get data and check content
                try:
                    first_chunk = next(response.iter_content(chunk_size=2048), b'')
                    if not first_chunk:
                        return False, "No data received"
                except:
                    return False, "Failed to read data"
                
                # Analyze what type of stream this might be
                stream_indicators = {
                    'hls': (
                        b'#EXTM3U' in first_chunk or 
                        'application/vnd.apple.mpegurl' in content_type or 
                        'application/x-mpegurl' in content_type or
                        url_lower.endswith('.m3u8')
                    ),
                    'dash': (
                        'application/dash+xml' in content_type or
                        url_lower.endswith('.mpd')
                    ),
                    'video_file': (
                        'video/' in content_type or
                        any(url_lower.endswith(ext) for ext in ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm'])
                    ),
                    'transport_stream': (
                        'video/mp2t' in content_type or
                        url_lower.endswith('.ts')
                    ),
                    'media_container': (
                        b'ftyp' in first_chunk[:100] or  # MP4
                        first_chunk.startswith(b'FLV') or  # FLV
                        b'ID3' in first_chunk[:100] or  # Media with ID3
                        first_chunk.startswith(b'\x00\x00\x01\xba') or  # MPEG PS
                        first_chunk.startswith(b'G\x40')  # Transport Stream
                    ),
                    'rtmp_like': url_lower.startswith(('rtmp://', 'rtmps://', 'rtsp://'))
                }
                
                # Log what we detected
                detected_types = [k for k, v in stream_indicators.items() if v]
                debug_print_sync(f"Stream type detection: {detected_types}, Content-Type: {content_type}")
                
                # If we have ffprobe available, validate with it (ffprobe can handle most formats)
                if self.ffprobe_available:
                    debug_print_sync("HTTP test passed, validating with ffprobe...")
                    return self._validate_with_ffprobe(stream_url)
                
                # Without ffprobe, accept if we got valid HTTP response with data
                # This is more permissive - if server responds with data, consider it potentially valid
                if len(first_chunk) > 0:
                    debug_print_sync("HTTP test passed, no ffprobe available - accepting based on data received")
                    return True, ""
                else:
                    return False, "No data received from stream"
                        
        except requests.exceptions.Timeout:
            return False, "HTTP timeout"
        except requests.exceptions.ConnectionError:
            return False, "Connection error"
        except Exception as e:
            return False, f"HTTP error: {str(e)}"
    
    def _validate_with_ffprobe(self, stream_url: str) -> Tuple[bool, str]:
        """Use ffprobe to validate stream content"""
        try:
            cmd = [
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_streams',
                '-select_streams', 'v:0',
                '-analyzeduration', '5000000',  # 3 seconds
                '-probesize', '5000000',        # 3MB
                stream_url
            ]
            
            result = subprocess.run(cmd, 
                                  capture_output=True, 
                                  text=True, 
                                  timeout=self.timeout)
            
            if result.returncode != 0:
                # ffprobe failed, but HTTP was OK - might still be valid
                return True, ""  # Don't fail just because ffprobe couldn't analyze it
            
            try:
                probe_data = json.loads(result.stdout)
                streams = probe_data.get('streams', [])
                video_streams = [s for s in streams if s.get('codec_type') == 'video']
                
                if not video_streams:
                    # No video streams found, but might be audio-only or playlist
                    return True, ""  # Don't fail - HTTP test already passed
                
                # Check if we have a valid video codec
                codec_name = video_streams[0].get('codec_name')
                if codec_name and codec_name != 'unknown':
                    debug_print_sync(f"Valid video stream confirmed: {codec_name}")
                    return True, ""
                else:
                    # Unknown codec, but HTTP was valid
                    return True, ""
                
            except json.JSONDecodeError:
                # Failed to parse ffprobe output, but HTTP was OK
                return True, ""
                
        except subprocess.TimeoutExpired:
            # ffprobe timeout, but HTTP was valid
            return True, ""
        except Exception:
            # ffprobe error, but HTTP was valid
            return True, ""
    
    def _test_http_only(self, stream_url: str) -> Tuple[bool, str]:
        """Fallback HTTP-only test"""
        try:
            headers = {
                'User-Agent': 'VLC/3.0.21 LibVLC/3.0.21',
                'Range': 'bytes=0-1023',
            }
            
            response = requests.get(stream_url, 
                                  headers=headers, 
                                  timeout=self.timeout, 
                                  stream=True,
                                  allow_redirects=True)
            
            if response.status_code not in [200, 206]:
                return False, f"HTTP error: {response.status_code}"
            
            return True, ""
            
        except requests.exceptions.Timeout:
            return False, "HTTP timeout"
        except requests.exceptions.ConnectionError:
            return False, "Connection error"
        except Exception as e:
            return False, f"HTTP error: {str(e)}"
    
    def test_stream(self, stream_data: Dict[str, Any]) -> Tuple[bool, str]:
        """Test a single stream - HTTP first, then ffprobe on same connection if available"""
        stream_url = stream_data.get('s_stream_uri', '')
        
        if not stream_url:
            return False, "No stream URL"
        
        # Try HTTP + ffprobe approach first
        try:
            is_valid, error = self._test_with_http_then_ffprobe(stream_url)
            return is_valid, error
        except Exception as e:
            # If the combined approach fails, fall back to simple HTTP test
            debug_print_sync(f"Combined test failed, trying HTTP only: {e}")
            return self._test_http_only(stream_url)