#!/usr/bin/env python3

# necessary imports
import time
import threading
from collections import OrderedDict
from typing import Any, Callable, Optional

# our caching class
class KP_Cache:

    # initialize the cache    
    def __init__( self, max_size: int = 1000, default_ttl: float = 3600.0 ):

        # setup the internal variables
        self._cache = OrderedDict( )
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._lock = threading.RLock( )
        self._expiration_callbacks = {}

    # set an item in the cache        
    def set( self, key: Any, value: Any, ttl_seconds: Optional[float] = None, on_expire: Optional[Callable[[Any, Any], None]] = None ) -> None:

        # with the thread lock
        with self._lock:

            # Evict expired and least recently used items if we're at capacity
            if len( self._cache ) >= self.max_size:
                self._evict( )
                
            # Use custom TTL if provided, otherwise use default
            expiration = time.time( ) + ( ttl_seconds if ttl_seconds is not None else self.default_ttl )
            self._cache[key] = {
                'value': value,
                'expiration': expiration
            }
            
            # Set expiration callback if provided
            if on_expire is not None:
                self._expiration_callbacks[key] = on_expire

            # otherwise remove any existing callback
            elif key in self._expiration_callbacks:
                del self._expiration_callbacks[key]
                
            # Move to end to show it was recently used
            self._cache.move_to_end( key )
        
    # get an item from the cache
    def get( self, key: Any ) -> Any:

        # with the thread lock
        with self._lock:

            # If key doesn't exist, return None
            if key not in self._cache:
                return None

            # Get the item
            item = self._cache[key]
            
            # Check if item has expired
            if item['expiration'] < time.time( ):

                # Execute callback if it exists
                self._execute_callback( key, item['value'] )

                # Remove expired item
                del self._cache[key]

                # Remove callback if it exists
                if key in self._expiration_callbacks:
                    del self._expiration_callbacks[key]

                # return nothing
                return None
                
            # Move to end to show it was recently used
            self._cache.move_to_end( key )

            # return the cached item
            return item['value']
        
    # delete an item from the cache
    def delete( self, key: Any ) -> None:

        # with the thread lock
        with self._lock:

            # if the key exists
            if key in self._cache:

                # delete it
                del self._cache[key]

            # Remove callback if it exists
            if key in self._expiration_callbacks:
                del self._expiration_callbacks[key]
            
    # clear the entire cache
    def clear( self ) -> None:

        # with the thread lock
        with self._lock:

            # Execute callbacks for all items being cleared
            for key, item in self._cache.items( ):

                # Execute callback if it exists
                if key in self._expiration_callbacks:
                    self._execute_callback( key, item['value'] )
            
            # Clear the cache and expiration callbacks
            self._cache.clear( )
            self._expiration_callbacks.clear( )
                
    # get the keys in the cache
    def keys( self ) -> list:

        # with the thread lock
        with self._lock:

            # setup the current time
            now = time.time( )

            # hold the valid and expired keys
            valid_keys = []
            expired_keys = []
            
            # for each item in the cache
            for key, item in self._cache.items( ):

                # if the item has expired
                if item['expiration'] < now:

                    # add it to the expired keys
                    expired_keys.append( key )

                # otherwise
                else:

                    # add it to the valid keys
                    valid_keys.append( key )
                    
            # loop through the expired keys
            for key in expired_keys:

                # Execute callback if it exists
                self._execute_callback( key, self._cache[key]['value'] )

                # Remove expired item
                del self._cache[key]

                # Remove callback if it exists
                if key in self._expiration_callbacks:
                    del self._expiration_callbacks[key]
                    
            # return the valid keys
            return valid_keys

    # set a callback for an item in the cache
    def set_callback( self, key: Any, callback: Optional[Callable[[Any, Any], None]] ) -> None:
        
        # with the thread lock
        with self._lock:

            # if the key exists
            if key in self._cache:

                # if the callback exists
                if callback is not None:
                    self._expiration_callbacks[key] = callback

                # otherwise remove any existing callback
                elif key in self._expiration_callbacks:
                    del self._expiration_callbacks[key]

    # set the default TTL for the cached items in seconds
    def set_default_ttl( self, ttl_seconds: float ) -> None:

        # with the thread lock
        with self._lock:
            self.default_ttl = ttl_seconds

    # setup item eviction
    def _evict( self ) -> None:

        # with the thread lock
        with self._lock:

            # setup the current time
            now = time.time( )

            # setup the expired keys
            expired_keys = [
                key for key, item in self._cache.items( )
                if item['expiration'] < now
            ]

            # loop over the expired keys
            for key in expired_keys:

                # Execute callback if it exists
                if key in self._expiration_callbacks:
                    self._execute_callback( key, self._cache[key]['value'] )

                # Remove expired item
                del self._cache[key]

                # Remove callback if it exists
                if key in self._expiration_callbacks:
                    del self._expiration_callbacks[key]
            
            # If we're over capacity
            while len( self._cache ) >= self.max_size:

                # Pop the oldest item (FIFO)
                key, item = self._cache.popitem( last=False )

                # Execute callback if it exists
                if key in self._expiration_callbacks:
                    self._execute_callback( key, item['value'] )

                    # Remove callback
                    del self._expiration_callbacks[key]
            
    # execute the callback
    def _execute_callback( self, key: Any, value: Any ) -> None:

        # setup the callback
        callback = self._expiration_callbacks.get( key )

        # if there is a callback
        if callback:

            # attemp to execute it
            try:

                callback( key, value )

            # trap any exceptions
            except Exception as e:

                # Don't let callback exceptions break cache operations
                import traceback
                traceback.print_exc( )
            
    # check if a key exists in the cache
    def __contains__( self, key: Any ) -> bool:
        
        # with the thread lock
        with self._lock:

            # if the key doesn't exist
            if key not in self._cache:
                return False
                
            # Get the item
            item = self._cache[key]

            # Check if item has expired
            if item['expiration'] < time.time( ):

                # Execute callback if it exists
                self._execute_callback( key, item['value'] )

                # Remove expired item
                del self._cache[key]

                # Remove callback if it exists
                if key in self._expiration_callbacks:
                    del self._expiration_callbacks[key]

                # return False
                return False
                
            # by default, return true
            return True
        
    # get the number of items in the cache
    def __len__( self ) -> int:
        
        # with the thread lock
        with self._lock:

            # return the length of the cache
            return len( self._cache )
        
