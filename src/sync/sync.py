#!/usr/bin/env python3

import sys
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

# Import debug utilities
try:
    from utils.debug import debug_print_sync
except ImportError:
    def debug_print_sync(msg): pass

# our sync class
class KP_Sync:

    # fire us up
    def __init__( self, max_threads=None ):

        # class imports
        from common.common import KP_Common
        self.common = KP_Common( )

        debug_print_sync("Initializing KP_Sync")

        # Thread pool configuration
        cpu_count = os.cpu_count( ) or 1
        default_threads = min( 8, max( 4, cpu_count * 2 ) )  # Better default range
        self.max_threads = self._determine_thread_count( max_threads, default_threads )
        
        debug_print_sync(f"Using {self.max_threads} threads for sync operations")
        
        # Initialize components
        from utils.cache import KP_Cache
        _cache = KP_Cache( )

        # clear our cache
        debug_print_sync("Clearing cache")
        _cache.clear( )
        del _cache

        # setup the internal sync data
        from sync.data import KP_Sync_Data
        self._data = KP_Sync_Data( )

        # setup the thread locks
        self._thread_lock = threading.Lock( )
        self._db_lock = threading.Lock( )

        debug_print_sync("KP_Sync initialization completed")

    # our main public sync function
    def sync( self ):
        
        debug_print_sync("Starting sync operation")
        
        # get our providers, and make sure we have some before proceeding
        _providers = self._data._get_providers( self.common.args.provider )
        if not _providers:
            self.common.kp_print( "error", "No providers found" )
            sys.exit( 1 )

        debug_print_sync(f"Found {len(_providers)} providers to process")

        # Show initial sync message
        self.common.kp_print_line( )
        self.common.kp_print( "info", "STARTING PROVIDER SYNC" )
        self.common.kp_print( "info", "Providers to process:" )
        for prov in _providers:
            self.common.kp_print( "info", f"- {prov['sp_name']}" )
        self.common.kp_print_line( )

        # hold our start time
        start_time = time.time( )

        # setup the results and error internals
        results = []
        has_errors = False
        
        debug_print_sync("Starting thread pool execution")
        
        # with our thread executor
        with ThreadPoolExecutor( max_workers=self.max_threads ) as executor:

            # setup the executions we're taking
            futures = {executor.submit( self._process_provider, prov ): prov['sp_name'] 
                      for prov in _providers}
            
            debug_print_sync(f"Submitted {len(futures)} provider processing tasks")
            
            # for each completed action: 1 hour timeout
            for future in as_completed( futures, timeout=3600 ):

                res = future.result( )

                if res is not None:

                    # setup the results
                    total, filtered, name, error = res

                    debug_print_sync(f"Provider {name} completed: {filtered}/{total} streams, error: {error}")

                    # oofff... if we have an error
                    if error:

                        # set the flag, and show the error
                        has_errors = True
                        self.common.kp_print( "error", f"Error processing {name}: {error}" )
                    
                    # append our results
                    results.append( ( total, filtered, name, error ) )

        debug_print_sync("Thread pool execution completed")

        # Final operations
        try:

            debug_print_sync("Starting final database operations")

            # utilize the database thread locker
            with self._db_lock:

                # sync the streams
                debug_print_sync("Syncing streams to database")
                self._data._sync_the_streams( )

                # clean up the streams
                debug_print_sync("Cleaning up streams")
                self.cleanup( )

                # attempt to fix up some data in the streams
                debug_print_sync("Running fixup operations")
                self.fixup( )

            debug_print_sync("Final database operations completed")

        # yikes, there was an error
        except Exception as e:
            has_errors = True
            self.common.kp_print( "error", f"Final operations failed: {str(e)}" )
            debug_print_sync(f"Final operations error: {e}")

        # Show final summary
        self._print_final_summary( results, time.time( ) - start_time, has_errors )

    # clean up the data
    def cleanup( self ):
        
        debug_print_sync("Running cleanup operations")
        # run it
        self._data._cleanup( )

    # fixup the data
    def fixup( self ):

        debug_print_sync("Running fixup operations")
        # run it
        self._data._fixup( )

    # determin the thread optimal thread count
    def _determine_thread_count( self, max_threads, default ):

        # if we do not have something configured
        if max_threads is not None:

            # return a default
            thread_count = max( 1, min( int( max_threads ), 16 ) )
            debug_print_sync(f"Using configured thread count: {thread_count}")
            return thread_count
        
        # otherwise, return the set default
        debug_print_sync(f"Using default thread count: {default}")
        return default

    # process a provider
    def _process_provider( self, _prov ):

        debug_print_sync(f"Processing provider: {_prov['sp_name']}")

        # try to process
        try:

            # with our database thread lockers
            with self._db_lock:

                debug_print_sync(f"Getting filters for provider {_prov['sp_name']}")
                # try to grab the users filters
                _filters = self._data._get_filters( _prov["u_id"] )
                if _filters is None:
                    debug_print_sync(f"No filters found for provider {_prov['sp_name']}")
                    return ( 0, 0, _prov['sp_name'], "No filters found" )

                debug_print_sync(f"Found {len(_filters)} filters for provider {_prov['sp_name']}")

            # Get and process streams
            from sync.get import KP_Get
            _get = KP_Get( )

            debug_print_sync(f"Fetching streams for provider {_prov['sp_name']}")
            # get the streams from the provider
            _streams = _get.get_streams( _prov )
            
            debug_print_sync(f"Retrieved {len(_streams)} streams for {_prov['sp_name']}")
            
            # setup the filtering
            from sync.filter import KP_Filter

            debug_print_sync(f"Applying filters to streams for {_prov['sp_name']}")
            # filter the streams
            _filtered_streams = KP_Filter.filter_streams( _streams, _filters )

            debug_print_sync(f"Filtered to {len(_filtered_streams)} streams for {_prov['sp_name']}")

            # now convert them to our common format
            _converted_streams = self._convert_streams( _filtered_streams, _prov )
            
            debug_print_sync(f"Converted {len(_converted_streams)} streams for {_prov['sp_name']}")
            
            # make sure we actually have converted streams
            if _converted_streams:

                # with out database lock
                with self._db_lock:

                    debug_print_sync(f"Inserting streams to database for {_prov['sp_name']}")
                    # insert the streams
                    self._data._insert_the_streams( _converted_streams )

                    debug_print_sync(f"Updating last synced time for {_prov['sp_name']}")
                    # update the last synced
                    self._data._update_last_synced( _prov["u_id"] )
            
            debug_print_sync(f"Provider {_prov['sp_name']} processing completed successfully")
            # return the streams
            return ( len( _streams ), len( _converted_streams ), _prov['sp_name'], None )
            
        # whoops... 
        except Exception as e:
            debug_print_sync(f"Provider {_prov['sp_name']} processing failed: {e}")
            return ( 0, 0, _prov['sp_name'], str( e ) )

    # setup and format the final "report"
    def _print_final_summary( self, results, total_time, has_errors ):

        # THIS IS THE ONLY OUTPUT THAT SHOULD SHOW WITHOUT --debug
        # Keep this visible for users
        
        self.common.kp_print_line( )
        self.common.kp_print("info", "SYNC SUMMARY")
        self.common.kp_print_line( )
        
        # how many were sucessful
        successful = [r for r in results if not r[3]]

        # how many faild
        failed = [r for r in results if r[3]]
        
        # if we have successes
        if successful:
            self.common.kp_print( "info", "SUCCESSFUL PROVIDERS:" )

            # loop the successes
            for total, filtered, name, _ in successful:

                # print out what the were with the stats
                self.common.kp_print( "info", f"- {name}: {total}/{filtered} streams" )
        
        # if we 
        if failed:
            self.common.kp_print( "info", "FAILED PROVIDERS:" )

            # loop the failures
            for _, _, name, error in failed:

                # print em out
                self.common.kp_print( "info", f"- {name} ({error})" )
        
        self.common.kp_print( "info", "\nSTATISTICS:" )
        self.common.kp_print( "info", f"Total providers: {len(results)}" )
        self.common.kp_print( "info", f"Successful: {len(successful)}" )
        self.common.kp_print( "info", f"Failed: {len(failed)}" )
        self.common.kp_print( "info", f"Total time: {total_time:.1f} seconds" )
        
        # if we had errors
        if has_errors:
            self.common.kp_print( "warn", "SYNC COMPLETED WITH ERRORS" )

        # otherwise
        else:
            self.common.kp_print( "success", "SYNC COMPLETED SUCCESSFULLY" )
            
        self.common.kp_print_line( )

    # convert our streams to a standardized format
    def _convert_streams( self, streams, provider ):

        debug_print_sync(f"Converting {len(streams)} streams for provider {provider['sp_name']}")

        # return the formatted streams        
        converted = [{
            'u_id': provider['u_id'],
            'p_id': provider['id'],
            's_orig_name': stream['stream_name'],
            's_stream_uri': stream['stream_url'],
            's_type_id': stream['stream_type'],
            's_tvg_id': stream['epg_id'],
            's_tvg_logo': stream['stream_icon'],
            's_extras': '',
            's_group': stream['stream_group'],
        } for _, stream in streams.items( )]
        
        debug_print_sync(f"Converted {len(converted)} streams successfully")
        return converted