#!/usr/bin/env python3

import sys
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

# our sync class
class KP_Sync:

    # fire us up
    def __init__( self, max_threads=None ):

        # class imports
        from common.common import KP_Common
        self.common = KP_Common( )

        # Thread pool configuration
        cpu_count = os.cpu_count( ) or 1
        default_threads = min( 8, max( 4, cpu_count * 2 ) )  # Better default range
        self.max_threads = self._determine_thread_count( max_threads, default_threads )
        
        # Initialize components
        from utils.cache import KP_Cache
        _cache = KP_Cache( )

        # clear our cache
        _cache.clear( )
        del _cache

        # setup the internal sync data
        from sync.data import KP_Sync_Data
        self._data = KP_Sync_Data( )

        # setup the thread locks
        self._thread_lock = threading.Lock( )
        self._db_lock = threading.Lock( )

    # our main public sync function
    def sync( self ):
        
        # get our providers, and make sure we have some before proceeding
        _providers = self._data._get_providers( self.common.args.provider )
        if not _providers:
            self.common.kp_print( "error", "No providers found" )
            sys.exit( 1 )

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
        
        # with our thread executor
        with ThreadPoolExecutor( max_workers=self.max_threads ) as executor:

            # setup the executions we're taking
            futures = {executor.submit( self._process_provider, prov ): prov['sp_name'] 
                      for prov in _providers}
            
            # for each completed action: 1 hour timeout
            for future in as_completed( futures, timeout=3600 ):

                res = future.result( )

                if res is not None:

                    # setup the results
                    total, filtered, name, error = res

                    # oofff... if we have an error
                    if error:

                        # set the flag, and show the error
                        has_errors = True
                        self.common.kp_print( "error", f"Error processing {name}: {error}" )
                    
                    # append our results
                    results.append( ( total, filtered, name, error ) )

        # Final operations
        try:

            # utilize the database thread locker
            with self._db_lock:

                # sync the streams
                self._data._sync_the_streams( )

                # clean up the streams
                self.cleanup( )

                # attempt to fix up some data in the streams
                self.fixup( )

        # yikes, there was an error
        except Exception as e:
            has_errors = True
            self.common.kp_print( "error", f"Final operations failed: {str(e)}" )

        # Show final summary
        self._print_final_summary( results, time.time( ) - start_time, has_errors )

    # clean up the data
    def cleanup( self ):
        
        # run it
        self._data._cleanup( )

    # fixup the data
    def fixup( self ):

        # run it
        self._data._fixup( )

    # determin the thread optimal thread count
    def _determine_thread_count( self, max_threads, default ):

        # if we do not have something configured
        if max_threads is not None:

            # return a default
            return max( 1, min( int( max_threads ), 16 ) )
        
        # otherwise, return the set default
        return default

    def _process_provider( self, _prov ):
        try:
            # Get filters
            with self._db_lock:
                _filters = self._data._get_filters( _prov["u_id"] )
                if _filters is None:
                    return ( 0, 0, _prov['sp_name'], "No filters found" )

            # Get and process streams
            from sync.get import KP_Get
            _get = KP_Get( )

            # get the streams from the provider
            _streams = _get.get_streams( _prov )
            #print(f"SYNC: Got {len(_streams)} streams from get_streams()")
            
            # setup the filtering
            from sync.filter import KP_Filter

            # filter the streams
            _filtered_streams = KP_Filter.filter_streams( _streams, _filters )
            #print(f"SYNC: After filtering: {len(_filtered_streams)} streams remain")

            # now convert them to our common format
            _converted_streams = self._convert_streams( _filtered_streams, _prov )
            #print(f"SYNC: After conversion: {len(_converted_streams)} streams to insert")
            
            # make sure we actually have converted streams
            if _converted_streams:
                # with out database lock
                with self._db_lock:
                    # insert the streams
                    self._data._insert_the_streams( _converted_streams )
                    #print(f"SYNC: Successfully inserted {len(_converted_streams)} streams")

                    # update the last synced
                    self._data._update_last_synced( _prov["u_id"] )
            #else:
            #    print("SYNC: No converted streams to insert!")
            
            # return the streams
            return ( len( _streams ), len( _converted_streams ), _prov['sp_name'], None )
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return ( 0, 0, _prov['sp_name'], str( e ) )

    # setup and format the final "report"
    def _print_final_summary( self, results, total_time, has_errors ):

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
            self.common.kp_print( "warning", "SYNC COMPLETED WITH ERRORS" )

        # otherwise
        else:
            self.common.kp_print( "success", "SYNC COMPLETED SUCCESSFULLY" )
            
        self.common.kp_print_line( )

    # convert our streams to a standardized format
    def _convert_streams( self, streams, provider ):

        # return the formatted streams        
        return [{
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

