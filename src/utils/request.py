#!/usr/bin/env python3

import json, sys
import requests # type: ignore
from requests.adapters import HTTPAdapter # type: ignore
from urllib3.util.retry import Retry # type: ignore
import logging
from typing import Optional, Union, Any, Dict, List
from functools import partial

# Import debug utilities
try:
    from utils.debug import debug_print_request
except ImportError:
    def debug_print_request(msg): pass

# our json request class
class KP_Request:

    # initialize the class    
    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 0.3,
        timeout: int = 30,
        chunk_size: int = 8192,  # Larger default chunk size
        max_response_size: Optional[int] = 500 * 1024 * 1024,  # 500MB default limit
        max_chunks: Optional[int] = None,  # Limit number of chunks if needed
        pool_connections: int = 10,
        pool_maxsize: int = 10,
        pool_block: bool = False,
        default_headers: Optional[dict] = None,
    ):
        
        debug_print_request("Initializing KP_Request")
        
        # setup our variables
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.timeout = timeout
        self.chunk_size = chunk_size
        self.max_response_size = max_response_size
        self.max_chunks = max_chunks
        self.pool_connections = pool_connections
        self.pool_maxsize = pool_maxsize
        self.pool_block = pool_block
        self.default_headers = default_headers or {}
        self.session = self._create_session( )
        
        debug_print_request(f"KP_Request initialized with timeout={timeout}s, max_retries={max_retries}")

    # context management start
    def __enter__( self ):
        return self

    # end context management
    def __exit__(self, exc_type, exc_val, exc_tb):
        debug_print_request("Closing KP_Request session")
        self.close( )

    # force updating the headers
    def update_headers(self, new_headers: dict) -> None:
        
        debug_print_request(f"Updating headers: {list(new_headers.keys())}")
        
        # Update the default headers for the session
        self.default_headers.update( new_headers )
        self.session.headers.update( new_headers )

    # create a request session
    def _create_session( self ) -> requests.Session:
        
        debug_print_request("Creating requests session")
        
        # fire up the local session
        session = requests.Session( )

        # Apply default headers to the session
        if self.default_headers:
            debug_print_request(f"Applying default headers: {list(self.default_headers.keys())}")
            session.headers.update( self.default_headers )
                                   
        # setup the retry
        retry = Retry(
            total=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=[500, 502, 503, 504]
        )

        # Create adapter with connection pooling
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=self.pool_connections,
            pool_maxsize=self.pool_maxsize,
            pool_block=self.pool_block
        )
        
        debug_print_request(f"Created HTTP adapter with pool_connections={self.pool_connections}, pool_maxsize={self.pool_maxsize}")
        
        # Mount the adapter to the session for a specific prefix
        session.mount( 'http://', adapter )
        session.mount( 'https://', adapter )
        
        debug_print_request("Requests session created successfully")
        
        # return the session
        return session

    # confgure the connection pooling
    def _configure_pooling( self,
        pool_connections: Optional[int] = None,
        pool_maxsize: Optional[int] = None,
        pool_block: Optional[bool] = None
    ) -> None:
        
        debug_print_request("Reconfiguring connection pooling")
        
        # Update pooling configuration if provided
        if pool_connections is not None:
            self.pool_connections = pool_connections
        if pool_maxsize is not None:
            self.pool_maxsize = pool_maxsize
        if pool_block is not None:
            self.pool_block = pool_block
        
        # Recreate session with new pooling configuration
        self.close( )
        self.session = self._create_session( )

    # safely parse a json response
    def _safe_parse_json( self, response: requests.Response, max_size: Optional[int] = None ) -> Union[dict, list]:
        
        debug_print_request(f"Parsing JSON response (Content-Length: {response.headers.get('Content-Length', 'unknown')})")
        
        # setup a bytearray for the content
        content = bytearray( )

        # setup the bytes and chunk
        bytes_read = 0
        chunk_count = 0
        
        # try to parse the response
        try:

            # loop over each chunk in the response
            for chunk in response.iter_content( chunk_size=self.chunk_size ):
                
                # if we have a chunk
                if chunk:

                    # extend the returned content by the chunk
                    content.extend( chunk )

                    # how many bytes were read? and wha chunk are we at?
                    bytes_read += len( chunk )
                    chunk_count += 1
                    
                    # Check size limits
                    if max_size is not None and bytes_read > max_size:
                        debug_print_request(f"Response size limit exceeded: {bytes_read} > {max_size}")
                        raise ValueError( f"Response exceeded maximum size of {max_size} bytes" )
                    if self.max_chunks is not None and chunk_count > self.max_chunks:
                        debug_print_request(f"Chunk limit exceeded: {chunk_count} > {self.max_chunks}")
                        raise ValueError( f"Response exceeded maximum chunk count of {self.max_chunks}" )
            
            debug_print_request(f"JSON parsing completed: {bytes_read} bytes, {chunk_count} chunks")
            
            # return the json as a dict or list
            return json.loads( content.decode( 'utf-8' ) )
        
        # if we fail to decode content
        except UnicodeDecodeError as e:
            debug_print_request(f"Unicode decode error: {e}")
            raise ValueError( f"Failed to decode response content: {str(e)}" ) from e
        
        # if we fail to decode the json
        except json.JSONDecodeError as e:
            debug_print_request(f"JSON decode error: {e}")
            raise ValueError(f"Failed to parse JSON: {str(e)}") from e

    # GET the remote json
    def get_json(
        self,
        url: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
        timeout: Optional[int] = None,
        max_size_override: Optional[int] = None
    ) -> Union[dict, list]:

        debug_print_request(f"GET JSON request to: {url}")
        
        # setup our config options
        timeout = timeout or self.timeout
        headers = headers or {}
        max_size = max_size_override or self.max_response_size

        # setup the Accept header
        headers.setdefault( 'Accept', 'application/json' )
        
        # try to get the json        
        try:

            # setup and configure our session to retrieve the json
            response = self.session.get(
                url,
                params=params,
                headers=headers,
                stream=True,
                timeout=timeout
            )

            debug_print_request(f"Response status: {response.status_code}")
            
            # setup the exceptions to be raised on certain HTTP response status codes
            response.raise_for_status( )
            
            # Get content length if available
            content_length = response.headers.get('Content-Length')
            
            # if we have a content length
            if content_length:

                # make sure it's an integer
                content_length = int( content_length )
                debug_print_request(f"Content-Length: {content_length}")

                # if it's greater than our max size
                if max_size is not None and content_length > max_size:
                    debug_print_request(f"Content length exceeds max size: {content_length} > {max_size}")
                    raise ValueError( f"Content-Length {content_length} exceeds maximum {max_size}" )
            
            # return the safely parsed json
            return self._safe_parse_json( response, max_size )
            
        # oof... there was an exception thrown for the request
        except requests.exceptions.RequestException as e:
            debug_print_request(f"Request exception: {e}")
            logging.error( f"Request to {url} failed: {str(e)}" )
            raise

        # oof... there was an exception thrown for the response
        except ValueError as e:
            debug_print_request(f"Value error during processing: {e}")
            logging.error( f"Response processing failed: {str(e)}" )
            raise

     # GET the remote text content
    def get_text(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        max_size_override: Optional[int] = None
    ) -> str:

        debug_print_request(f"GET text request to: {url}")

        # setup our config options
        timeout = timeout or self.timeout
        headers = headers or {}
        max_size = max_size_override or self.max_response_size

        # setup the Accept header
        headers.setdefault( 'Accept', 'text/plain' )

        # try to get the text content        
        try:

            # setup and configure our session to retrieve the text
            response = self.session.get(
                url,
                params=params,
                headers=headers,
                stream=True,
                timeout=timeout
            )

            debug_print_request(f"Response status: {response.status_code}")

            # setup the exceptions to be raised on certain HTTP response status codes
            response.raise_for_status( )
            
            # Get content length if available
            content_length = response.headers.get('Content-Length')
            
            # if we have a content length
            if content_length:

                # make sure it's an integer
                content_length = int( content_length )
                debug_print_request(f"Content-Length: {content_length}")

                # if it's greater than our max size
                if max_size is not None and content_length > max_size:
                    debug_print_request(f"Content length exceeds max size: {content_length} > {max_size}")
                    raise ValueError( f"Content-Length {content_length} exceeds maximum {max_size}" )
            
            # return the safely parsed text
            #return self._safe_parse_text( response, max_size )
            text_content = response.text
            debug_print_request(f"Retrieved text content: {len(text_content)} characters")
            return text_content

        # oof... there was an exception thrown for the request
        except requests.exceptions.RequestException as e:
            debug_print_request(f"Request exception: {e}")
            logging.error( f"Request to {url} failed: {str(e)}" )
            raise

        # oof... there was an exception thrown for the response
        except ValueError as e:
            debug_print_request(f"Value error during processing: {e}")
            logging.error( f"Response processing failed: {str(e)}" )
            raise

    # close our session
    def close( self ) -> None:
        debug_print_request("Closing requests session")
        self.session.close( )